package jarvey.streams.updatelog;

import static utils.Utilities.checkNotNullArgument;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KeyValue;

import com.google.common.collect.Maps;

import utils.func.FOption;
import utils.jdbc.JdbcProcessor;
import utils.jdbc.JdbcRowSource;
import utils.stream.FStream;

import jarvey.streams.model.Range;
import jarvey.streams.rx.KafkaPoller;
import jarvey.streams.rx.KafkaUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KeyedUpdateLogs<T extends KeyedUpdate> {
	private static final KafkaPoller DEFAULT_POLLER = KafkaPoller.stopOnTimeout(Duration.ofSeconds(1))
																	.initialTimeout(Duration.ofSeconds(5));
	
	private final JdbcProcessor m_jdbc;
	private final String m_idxTableName;
	private KafkaPoller m_poller = DEFAULT_POLLER;

	private final Properties m_consumerProps;
	private final String m_topic;
	private final Deserializer<T> m_eventDeserializer;
	
	/**
	 * Kafka topic에 저장된 update log를 접근하는 {@link KeyedUpdateLogs} 객체를 생성한다.
	 *
	 * @param jdbc				Database 접근에 사용한 JDBC 처리기 객체.
	 * @param idxTableName		index가 저장된 database table 이름.
	 * @param consumerProps		Kafka topic 접근을 위한 Kafka 접속 정보.
	 * @param topic				update log가 기록된 Kafka topic 이름.
	 * @param eventDeserializer	 Kafka topic에 저장된 record 접근을 위한 record deserializer.
	 */
	public KeyedUpdateLogs(JdbcProcessor jdbc, String idxTableName,
							Properties consumerProps, String topic, Deserializer<T> eventDeserializer) {
		m_consumerProps = consumerProps;
		m_idxTableName = idxTableName;
		m_topic = topic;
		m_jdbc = jdbc;
		m_eventDeserializer = eventDeserializer;
	}
	
	public void setKafkaPoller(KafkaPoller poller) {
		m_poller = poller;
	}
	
	/**
	 * 주어진 식별자에 해당하는 {@link KeyedUpdateIndex}를 반환한다.
	 * 
	 * @param key	update 식별자.
	 * @return	식별자에 해당하는 update index가 존재하면 FOption<KeyedUpdateIndex>,
	 * 			그렇지 않으면 {@link FOption#empty()}
	 */
	public FOption<KeyedUpdateIndex> readIndex(@Nonnull String key) {
		final String sql = String.format("select * from %s where key=?", m_idxTableName);
		
		return JdbcRowSource.select(this::toIndex)
							.from(m_jdbc)
							.executeQuery(conn -> {
								PreparedStatement pstmt = conn.prepareStatement(sql);
								pstmt.setString(1, key);
								return pstmt.executeQuery();
							})
							.first();
	}
	
	/**
	 * 등록된 모든 update index들의 스트림을 반환한다.
	 * 
	 * @return {@code FStream<KeyedUpdateIndex>} 객체.
	 */
	public FStream<KeyedUpdateIndex> fstream() {
		String sqlString = String.format("select * from %s", m_idxTableName);
		return m_jdbc.streamQuery(sqlString, this::toIndex);
	}
	
	/**
	 * 주어진 식별자에 해당하는 모든 update들을 스트림을 반환한다.
	 * 
	 * @param key	검색 대상 update의 식별자.
	 * @return	Kafka topic에 저장된 변경 이벤트들 중에서 주어진 식별자에
	 * 			해당 객체에 대한 변경 이벤트들의 스트림을 반환한다.
	 */
	public FStream<KeyValue<String,T>> streamKeyedUpdatesOfKey(@Nonnull String key) {
		return readIndex(key)
					.flatMapFStream(this::streamUpdatesOfIndex);
	}
	
	/**
	 * 주어진 update index에 해당하는 Kafka topic에 저장된 update event들의 스트림을 반환한다.
	 * 
	 * @param index		검색 대상 update index
	 * @return		Update event 스트림.
	 */
	public FStream<KeyValue<String,T>> streamUpdatesOfIndex(@Nonnull KeyedUpdateIndex index) {
		checkNotNullArgument(index);
		
		TopicPartition tpart = new TopicPartition(m_topic, index.getPartitionNumber());
		long firstOffset = index.getTopicOffsetRange().min();
		long lastOffset = index.getTopicOffsetRange().max();
		
		return KafkaUtils
				.<String,byte[]>streamKafkaTopicPartions(m_consumerProps, tpart, firstOffset, m_poller)
				.takeWhile(rec -> rec.offset() <= lastOffset)
				.map(rec -> KeyValue.pair(rec.key(),
										m_eventDeserializer.deserialize(rec.topic(), rec.value())))
				.filter(kv -> index.getKey().contains(kv.value.getKey()));
	}
	
	public List<KeyedUpdateIndex> listKeyedUpdateIndex(String whereClause) {
		String sqlString = String.format("select * from %s where %s", m_idxTableName, whereClause);
		return m_jdbc.streamQuery(sqlString, this::toIndex).toList();
	}
	
	public List<KeyedUpdateIndex> listKeyedUpdateIndexStopBetween(Range<Long> tsRange) {
		String sqlString = String.format("select * from %s where last_ts between ? and ?", m_idxTableName);

		return JdbcRowSource.select(this::toIndex)
							.from(m_jdbc)
							.executeQuery(conn -> {
								PreparedStatement pstmt = conn.prepareStatement(sqlString);
								pstmt.setLong(1, tsRange.min());
								pstmt.setLong(2, tsRange.max());
								return pstmt.executeQuery();
							})
							.toList();
	}
	
	public List<KeyedUpdateIndex> listKeyedUpdateIndex(Range<Long> tsRange) {
		String sqlString = null;
		if ( tsRange.isInfiniteMin() && tsRange.isInfiniteMax() ) {
			sqlString = String.format("select * from %s", m_idxTableName);
		}
		else if ( tsRange.isInfiniteMin() ) {
			sqlString = String.format("select * from %s where last_ts <= %d", m_idxTableName, tsRange.max());
		}
		else if ( tsRange.isInfiniteMax() ) {
			sqlString = String.format("select * from %s where first_ts >= %d", m_idxTableName, tsRange.min());
		}
		else {
			sqlString = String.format("select * from %s where first_ts < %d and last_ts >= %d",
										m_idxTableName, tsRange.max(), tsRange.min());
		}
		
		return m_jdbc.streamQuery(sqlString, this::toIndex).toList();
	}
	
	public Map<String,KeyedUpdateIndex> readIndexes(Iterable<String> keys) {
		Map<String,KeyedUpdateIndex> indexes = Maps.newHashMap();
		
		try ( Connection conn = m_jdbc.connect() ) {
			String sql = String.format("select * from %s where key=?", m_idxTableName);
			try ( PreparedStatement pstmt = conn.prepareStatement(sql) ) {
				for ( String key: keys ) {
					pstmt.setString(1, key);
					try ( ResultSet rs = pstmt.executeQuery() ) {
						if ( rs.next() ) {
							indexes.put(key, toIndex(rs));
						}
					}
				}
			}
			
			return indexes;
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}
	
	private KeyedUpdateIndex toIndex(ResultSet rs) throws SQLException {
		String key = rs.getString(1);
		
		long minOffset = (rs.getLong(3) != -1) ? rs.getLong(3) : null;
		long maxOffset = (rs.getLong(4) != -1) ? rs.getLong(4) : null;
		long minTs = (rs.getLong(5) != -1) ? rs.getLong(5) : null;
		long maxTs = (rs.getLong(6) != -1) ? rs.getLong(6) : null;
		
		return new KeyedUpdateIndex(key, rs.getInt(2),
									Range.between(minOffset, maxOffset),
									Range.between(minTs, maxTs),
									rs.getInt(7));
	}
	
	private static final String SQL_CREATE_TABLE
		= "create table %s ("
		+ 	"key varchar not null, "
		+ 	"partition integer not null, "
		+ 	"first_offset long not null, "
		+ 	"last_offset long not null, "
		+ 	"first_ts long not null, "
		+ 	"last_ts long not null, "
		+ 	"length integer not null, "
		+ 	"primary key (key)"
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
