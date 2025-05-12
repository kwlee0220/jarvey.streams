package jarvey.streams.node;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import utils.LoggerSettable;
import utils.func.FOption;
import utils.func.Unchecked;
import utils.jdbc.JdbcProcessor;
import utils.stream.KeyValueFStream;

import jarvey.streams.model.JarveySerdes;
import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class NodeTrackIndexBuilder implements KafkaConsumerRecordProcessor<String,byte[]>, LoggerSettable {
	private static final Logger s_logger = LoggerFactory.getLogger(NodeTrackIndexBuilder.class);
	
	private final String m_indexTableName;
	private final Deserializer<NodeTrack> m_deser;
	private final JdbcProcessor m_jdbc;
	private Logger m_logger = s_logger;
	
	private final Map<String,NodeTrackletIndex> m_entries = Maps.newHashMap();
	private long m_lastOffset = 0;
	
	public NodeTrackIndexBuilder(String indexTable, JdbcProcessor jdbc) throws SQLException {
		m_indexTableName = indexTable;
		m_deser = JarveySerdes.NodeTrack().deserializer();
		m_jdbc = jdbc;
		
		prepare();
	}
	
	@Override
	public void close() throws Exception {
		unprepare();
	}

	@Override
	public ProcessResult process(TopicPartition tpart, ConsumerRecord<String, byte[]> record) {
		NodeTrack track = m_deser.deserialize(record.topic(), record.value());
		
		update(track, record.partition(), record.offset());

		// 등록된 index entry 중에서 가장 작은 offset을 찾는다.
		long offset = findOldestIndex().map(NodeTrackletIndex::getFirstTopicOffset)
										.ifAbsent(() -> unprepare())
										.getOrElse(record.offset());
		if ( offset > m_lastOffset ) {
			m_lastOffset = offset;
			return ProcessResult.of(tpart, offset + 1, false);
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
		NodeTrack track = m_deser.deserialize(record.topic(), record.value());
		return track.getTimestamp();
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	private void update(NodeTrack track, int partition, long offset) {
		boolean updated = false;
		
		String key = track.getKey();
		NodeTrackletIndex index = m_entries.get(key);
		if ( index == null ) {		// 주어진 key에 대한 첫번째 갱신인 경우.
			// 첫번째 갱신이 마지막(delete) 이벤트인 경우는 무시한다.
			if ( track.isLastUpdate() ) {
				return;
			}
			
			index = new NodeTrackletIndex(track, partition, offset);
			m_entries.put(key, index);
			try {
				insertIndex(index);
				if ( getLogger().isDebugEnabled() ) {
					getLogger().debug("create an index entry: key={}, ts={}, offset={}",
									key, track.getTimestamp(), offset);
				}
			}
			catch ( SQLException e ) {
				getLogger().error("fails to insert record: {}, cause={}", index, e);
			}
		}
		else {
			updated = index.update(track);
		}

		if ( track.isLastUpdate() ) {
			if ( index != null ) {
				try {
					index.lastUpdate(track, offset);
					updateIndex(index);
					m_entries.remove(key);
					if ( getLogger().isInfoEnabled() ) {
						getLogger().info("complete NodeTrackIndex: {}", index);
					}
				}
				catch ( SQLException e ) {
					getLogger().error("fails to complete NodeTrackIndex: {}, cause={}", index, e);
				}
			}
		}
		else if ( updated ) {
			try {
				updateIndex(index);
			}
			catch ( SQLException e ) {
				getLogger().error("fails to update NodeTrackIndex: {}, cause={}", index, e);
			}
		}
	}
	
	private FOption<NodeTrackletIndex> findOldestIndex() {
		return KeyValueFStream.from(m_entries)
								.map((k,v) -> v)
								.min(idx -> idx.getFirstTopicOffset());
	}

	private Connection m_conn = null;
	private PreparedStatement m_pstmtInsertIndex;
	private PreparedStatement m_pstmtUpdateIndex;
	
	private static final String SQL_INSERT_INDEX
		= "insert into %s (node, track_id, enter_zone, exit_zone, first_ts, last_ts, "
						+ "partition, first_offset, last_offset, count) "
			+	"values (?, ?, ?, ?, ?, -1, ?, ?, -1, ?) "
		+ "on conflict (node, track_id) "
		+ "do update set "
			+ "enter_zone=?, exit_zone=?, "
			+ "first_ts=?, last_ts=-1, "
			+ "partition=?, first_offset=?, last_offset=-1, "
			+ "count=?";
	private static final String SQL_UPDATE_INDEX
	= "update %s set enter_zone=?, exit_zone=?, last_ts=?, last_offset=?, count=? "
		+ "where node = ? and track_id = ?";
	
	private void prepare() throws SQLException {
		if ( m_conn == null ) {
			m_conn = m_jdbc.connect();
			
			String insertSql = String.format(SQL_INSERT_INDEX, m_indexTableName);
			m_pstmtInsertIndex = m_conn.prepareStatement(insertSql);

			String updateSql = String.format(SQL_UPDATE_INDEX, m_indexTableName);
			m_pstmtUpdateIndex = m_conn.prepareStatement(updateSql);
		}
	}
	private void unprepare() {
		if ( m_conn != null ) {
			Unchecked.runOrIgnore(m_pstmtInsertIndex::close);
			Unchecked.runOrIgnore(m_pstmtUpdateIndex::close);
			Unchecked.runOrIgnore(m_conn::close);
			m_conn = null;
		}
	}
	
	private void insertIndex(NodeTrackletIndex index) throws SQLException {
		prepare();
		
		m_pstmtInsertIndex.setString(1, index.getNodeId());
		m_pstmtInsertIndex.setString(2, index.getTrackId());
		m_pstmtInsertIndex.setString(3, index.getEnterZone());
		m_pstmtInsertIndex.setString(4, index.getExitZone());
		m_pstmtInsertIndex.setLong(5, index.getFirstTimestamp());
		m_pstmtInsertIndex.setLong(6, index.getPartitionNumber());
		m_pstmtInsertIndex.setLong(7, index.getFirstTopicOffset());
		m_pstmtInsertIndex.setInt(8, index.getUpdateCount());
		m_pstmtInsertIndex.setString(9, index.getEnterZone());
		m_pstmtInsertIndex.setString(10, index.getExitZone());
		m_pstmtInsertIndex.setLong(11, index.getFirstTimestamp());
		m_pstmtInsertIndex.setLong(12, index.getPartitionNumber());
		m_pstmtInsertIndex.setLong(13, index.getFirstTopicOffset());
		m_pstmtInsertIndex.setInt(14, index.getUpdateCount());
		m_pstmtInsertIndex.executeUpdate();
	}

	private void updateIndex(NodeTrackletIndex index) throws SQLException {
		prepare();

		m_pstmtUpdateIndex.setString(1, index.getEnterZone());
		m_pstmtUpdateIndex.setString(2, index.getExitZone());
		m_pstmtUpdateIndex.setLong(3, index.getLastTimestamp());
		m_pstmtUpdateIndex.setLong(4, index.getLastTopicOffset());
		m_pstmtUpdateIndex.setInt(5, index.getUpdateCount());
		m_pstmtUpdateIndex.setString(6, index.getNodeId());
		m_pstmtUpdateIndex.setString(7, index.getTrackId());
		m_pstmtUpdateIndex.executeUpdate();
	}
	
	private static final String SQL_CREATE_TABLE
		= "create table %s ("
		+ 	"node varchar not null, "			// 1
		+ 	"track_id varchar not null, "		// 2
		+ 	"enter_zone varchar, "				// 3
		+ 	"exit_zone varchar, "				// 4
		+ 	"first_ts bigint not null, "		// 5
		+ 	"last_ts bigint not null, "			// 6
		+ 	"partition integer not null, "		// 7
		+ 	"first_offset bigint not null, "	// 8
		+ 	"last_offset bigint not null, "		// 9
		+ 	"count integer not null, "			// 10
		+ 	"primary key (node, track_id)"
		+ ")";
	
	public static void createIndexTable(Connection conn, String tableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(String.format(SQL_CREATE_TABLE, tableName));
	}
	
	public static void dropIndexTable(Connection conn, String tableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(String.format("drop table if exists %s", tableName));
	}
}
