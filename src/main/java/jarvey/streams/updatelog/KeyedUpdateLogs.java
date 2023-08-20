package jarvey.streams.updatelog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.func.KeyValue;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.streams.ConsumerRecordStream;
import jarvey.streams.model.Range;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KeyedUpdateLogs<T extends KeyedUpdate> {
	private final JdbcProcessor m_jdbc;
	private final String m_idxTableName;

	private final Properties m_consumerProps;
	private final String m_topic;
	private final Deserializer<T> m_deserializer;
	
	/**
	 * Kafka topic에 저장된 update log를 접근하는 {@link KeyedUpdateLogs} 객체를 생성한다.
	 *
	 * @param jdbc				Database 접근에 사용한 JDBC 처리기 객체.
	 * @param idxTableName		index가 저장된 database table 이름.
	 * @param consumerProps		Kafka topic 접근을 위한 Kafka 접속 정보.
	 * @param topic				update log가 기록된 Kafka topic 이름.
	 * @param deserializer		Kafka topic에 저장된 record 접근을 위한 record deserializer.
	 */
	public KeyedUpdateLogs(JdbcProcessor jdbc, String idxTableName,
							Properties consumerProps, String topic, Deserializer<T> deserializer) {
		m_consumerProps = consumerProps;
		m_idxTableName = idxTableName;
		m_topic = topic;
		m_jdbc = jdbc;
		m_deserializer = deserializer;
	}
	
	public FStream<T> stream(String key) {
		return stream(readIndex(key));
	}
	
	public FStream<KeyValue<String,T>> streamOfKeys(Iterable<String> keys) {
		return streamOfIndexes(readIndexes(keys).values());
	}
	
	public FStream<T> stream(KeyedUpdateIndex index) {
		TopicPartition tpart = new TopicPartition(m_topic, index.getPartitionNumber());
		ConsumerRecordStream stream 
			= ConsumerRecordStream.from(m_consumerProps)
									.addRange(tpart, index.getTopicOffsetRange())
									.build();
		
		return stream.map(rec -> m_deserializer.deserialize(rec.topic(), rec.value()))
					.filter(t -> index.getKey().contains(t.getKey()));
	}
	
	public FStream<KeyValue<String,T>> streamOfIndexes(Iterable<KeyedUpdateIndex> indexes) {
		Set<String> keys = FStream.from(indexes).map(KeyedUpdateIndex::getKey).toSet();
		
		ConsumerRecordStream.Builder builder = ConsumerRecordStream.from(m_consumerProps);
		for ( KeyedUpdateIndex idx: indexes ) {
			builder.addRange(new TopicPartition(m_topic, idx.getPartitionNumber()),
							idx.getTopicOffsetRange());
		}
		ConsumerRecordStream stream = builder.build();
		
		return stream.map(rec -> KeyValue.of(rec.key(), m_deserializer.deserialize(rec.topic(), rec.value())))
					.filter(kv -> keys.contains(kv.value().getKey()));
	}
	
	public List<KeyedUpdateIndex> listKeyedUpdateIndex(Range<Long> tsRange) {
		try ( Connection conn = m_jdbc.connect() ) {
			String sql = String.format("select key from %s where first_ts >= ? and last_ts <= ?",
										m_idxTableName);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, tsRange.min());
			pstmt.setLong(2, tsRange.max());
			
			return m_jdbc.executeQuery(pstmt)
							.mapOrThrow(this::readIndex)
							.toList();
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}
	
	private KeyedUpdateIndex readIndex(String key) {
		try ( Connection conn = m_jdbc.connect() ) {
			String sql = String.format("select * from %s where key=?", m_idxTableName);
			try ( PreparedStatement pstmt = conn.prepareStatement(sql) ) {
				pstmt.setString(1, key);
				
				ResultSet rs = pstmt.executeQuery();
				if ( rs.next() ) {
					return readIndex(rs);
				}
				else {
					return null;
				}
			}
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}
	
	private Map<String,KeyedUpdateIndex> readIndexes(Iterable<String> keys) {
		Map<String,KeyedUpdateIndex> indexes = Maps.newHashMap();
		
		try ( Connection conn = m_jdbc.connect() ) {
			String sql = String.format("select * from %s where key=?", m_idxTableName);
			try ( PreparedStatement pstmt = conn.prepareStatement(sql) ) {
				for ( String key: keys ) {
					pstmt.setString(1, key);
					try ( ResultSet rs = pstmt.executeQuery() ) {
						if ( rs.next() ) {
							indexes.put(key, readIndex(rs));
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
	
	private KeyedUpdateIndex readIndex(ResultSet rs) throws SQLException {
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
