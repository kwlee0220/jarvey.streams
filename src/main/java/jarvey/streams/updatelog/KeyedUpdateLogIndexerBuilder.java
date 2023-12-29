package jarvey.streams.updatelog;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import utils.func.FOption;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KeyedUpdateLogIndexerBuilder<T extends KeyedUpdate>
												implements KafkaConsumerRecordProcessor<String,byte[]> {
	private static final Logger s_logger = LoggerFactory.getLogger(KeyedUpdateLogIndexerBuilder.class);
	
	private final String m_indexTableName;
	
	private Deserializer<T> m_deser;
	private JdbcProcessor m_jdbc;
	
	private final Map<String,IndexRecord> m_entries = Maps.newHashMap();
	private long m_lastOffset = 0;
	
	public KeyedUpdateLogIndexerBuilder(String indexTable, Deserializer<T> deser, JdbcProcessor jdbc) {
		m_indexTableName = indexTable;
		m_deser = deser;
		m_jdbc = jdbc;
	}
	
	@Override
	public void close() throws Exception {
	}

	@Override
	public ProcessResult process(TopicPartition tpart, ConsumerRecord<String, byte[]> record) {
		T update = m_deser.deserialize(record.topic(), record.value());
		
		String key = update.getKey();
		IndexRecord index = m_entries.get(key);
		if ( index == null ) {		// 주어진 key에 대한 첫번째 갱신인 경우.
			// 첫번째 갱신이 마지막(delete) 이벤트인 경우는 무시한다.
			if ( !update.isLastUpdate() ) {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("create an index entry: key={}, ts={}, offset={}",
									key, update.getTimestamp(), record.offset());
				}
				
				index = new IndexRecord(key, record.partition(), record.offset(), update.getTimestamp());
				m_entries.put(key, index);
			}
		}
		else {
			index.update(record.offset(), update.getTimestamp());
		}

		if ( update.isLastUpdate() ) {
			if ( s_logger.isDebugEnabled() ) {
				s_logger.debug("finalize index entry: key={}, ts={}, offset={}, count={}",
								key, update.getTimestamp(), record.offset(), index.m_count);
			}

			if ( index != null ) {
				try {
					insertIndex(index);
					m_entries.remove(key);
				}
				catch ( SQLException e ) {
					s_logger.error("fails to insert record: {}, cause={}", index, e.getMessage());
				}
			}
		}
		
		long offset = getOldestIndex().map(idx -> idx.m_firstOffset).getOrElse(record.offset());
		if ( offset > m_lastOffset ) {
			m_lastOffset = offset;
			return ProcessResult.of(tpart, offset + 1);
		}
		else {
			return ProcessResult.NULL;
		}
	}

	@Override
	public ProcessResult timeElapsed(TopicPartition tpart, long expectedTs) {
		return ProcessResult.NULL;
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		T update = m_deser.deserialize(record.topic(), record.value());
		return update.getTimestamp();
	}
	
	private FOption<IndexRecord> getOldestIndex() {
		return FStream.from(m_entries)
						.map((k,v) -> v)
						.min(idx -> idx.m_firstOffset);
	}
	
	private static final class IndexRecord {
		private final String m_key;
		private final int m_partNo;
		private long m_firstOffset;
		private long m_lastOffset;
		private long m_firstTs;
		private long m_lastTs;
		private int m_count;
		
		IndexRecord(String key, int partNo, long firstOffset, long firstTs) {
			m_key = key;
			m_partNo = partNo;
			m_firstOffset = firstOffset;
			m_lastOffset = firstOffset;
			m_firstTs = firstTs;
			m_lastTs = firstTs;
			m_count = 1;
		}
		
		void update(long offset, long ts) {
			m_lastOffset = offset;
			m_lastTs = ts;
			m_count += 1;
		}
		
		@Override
		public String toString() {
			return String.format("%s{part=%d, offset=%d:%d, ts=%d:%d, count=%d}",
								m_key, m_partNo, m_firstOffset, m_lastOffset, m_firstTs, m_lastTs, m_count);
		}
	};

	private static final String SQL_INSERT_INDEX
		= "insert into %s (key, partition, first_offset, last_offset, first_ts, last_ts, count) "
			+	"values ('%s', %d, %d, %d, %d, %d, %d) "
		+ "on conflict (key) "
		+ "do update set "
			+ "partition=%d, "
			+ "first_offset=%d, last_offset=%d, "
			+ "first_ts=%d, last_ts=%d, "
			+ "count=%d";
	private void insertIndex(IndexRecord index) throws SQLException {
		String sql = String.format(SQL_INSERT_INDEX, m_indexTableName, index.m_key, index.m_partNo,
											index.m_firstOffset, index.m_lastOffset,
											index.m_firstTs, index.m_lastTs, index.m_count,
											index.m_partNo, index.m_firstOffset, index.m_lastOffset,
											index.m_firstTs, index.m_lastTs, index.m_count);
		m_jdbc.executeUpdate(sql);
	}

	private static final String SQL_CREATE_TABLE
		= "create table %s ("
		+ 	"key varchar not null, "
		+ 	"partition integer not null, "
		+ 	"first_offset bigint not null, "
		+ 	"last_offset bigint not null, "
		+ 	"first_ts bigint not null, "
		+ 	"last_ts bigint not null, "
		+ 	"count integer not null, "
		+ 	"primary key (key)"
		+ ")";
	public static void createTable(Connection conn, String indexTableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(String.format(SQL_CREATE_TABLE, indexTableName));
	}
	
	public static void dropTable(Connection conn, String indexTableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("drop table if exists " + indexTableName);
	}
}
