package jarvey.streams.node;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KeyValue;

import com.google.common.collect.Maps;

import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackletId;
import jarvey.streams.rx.KafkaPoller;
import jarvey.streams.rx.KafkaUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class NodeTrackletUpdateLogs {
	private final JdbcProcessor m_jdbc;
	private final String m_idxTableName;

	private final Properties m_consumerProps;
	private final String m_topic;
	private final Deserializer<NodeTrack> m_deserializer;
	
	/**
	 * Kafka topic에 저장된 update log를 접근하는 {@link NodeTrackletUpdateLogs} 객체를 생성한다.
	 *
	 * @param jdbc				Database 접근에 사용한 JDBC 처리기 객체.
	 * @param idxTableName		index가 저장된 database table 이름.
	 * @param consumerProps		Kafka topic 접근을 위한 Kafka 접속 정보.
	 * @param topic				update log가 기록된 Kafka topic 이름.
	 */
	public NodeTrackletUpdateLogs(JdbcProcessor jdbc, String idxTableName,
									Properties consumerProps, String topic) {
		m_jdbc = jdbc;
		m_idxTableName = idxTableName;
		m_consumerProps = consumerProps;
		m_topic = topic;
		m_deserializer = JarveySerdes.NodeTrack().deserializer();
	}
	
	public String getIndexTableName() {
		return m_idxTableName;
	}
	
	/**
	 * Tracklet 식별자에 해당하는 index 객체를 얻는다.
	 * 
	 * @param trkId		검색 키.
	 * @return	{@link NodeTrackletIndex}. TrackletId에 해당하는 index가 없는 경우는 {@code null}.
	 */
	public NodeTrackletIndex getIndex(TrackletId trkId) {
		final String sql = String.format("select * from %s where node=? and track_id=?", m_idxTableName);
		try ( Connection conn = m_jdbc.connect() ) {
			try ( PreparedStatement pstmt = conn.prepareStatement(sql) ) {
				pstmt.setString(1, trkId.getNodeId());
				pstmt.setString(2, trkId.getTrackId());
				
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
	
	public FStream<KeyValue<String,NodeTrack>> streamOfKey(TrackletId key) {
		return streamUpdatesOfIndex(getIndex(key));
	}
	
	/**
	 * 주어진 {@link NodeTrackletIndex}에 해당하는 {@link NodeTrack} 이벤트 스트림을 반환한다.
	 * 
	 * @param index	대상 node tracklet 인덱스.
	 * @return	대상 node tracklet에 해당하는 {@link NodeTrack} 이벤트와 해당 Kafka key pair 스트림.
	 */
	public FStream<KeyValue<String,NodeTrack>> streamUpdatesOfIndex(NodeTrackletIndex index) {
		TopicPartition tpart = new TopicPartition(m_topic, index.getPartitionNumber());
		long firstOffset = index.getFirstTopicOffset();
		long lastOffset = index.getLastTopicOffset();
		KafkaPoller poller = KafkaPoller.infinite(Duration.ofSeconds(5));
		return KafkaUtils
				.<String,byte[]>streamKafkaTopicPartions(m_consumerProps, tpart, firstOffset, poller)
				.takeWhile(rec -> rec.offset() <= lastOffset)
				.map(rec -> KeyValue.pair(rec.key(),
						m_deserializer.deserialize(rec.topic(), rec.value())))
				.filter(kv -> index.getTrackletId().equals(kv.value.getTrackletId()));
	}
	
	public List<NodeTrackletIndex> findIndexes(String whereClause) {
		String sqlString = String.format("select * from %s where %s", m_idxTableName, whereClause);
		return m_jdbc.streamQuery(sqlString, NodeTrackletUpdateLogs::readIndex).toList();
	}
	
	private Map<TrackletId,NodeTrackletIndex> readIndexes(Iterable<TrackletId> keys) {
		Map<TrackletId,NodeTrackletIndex> indexes = Maps.newHashMap();
		
		try ( Connection conn = m_jdbc.connect() ) {
			String sql = String.format("select * from %s where node=? and track_id=?", m_idxTableName);
			try ( PreparedStatement pstmt = conn.prepareStatement(sql) ) {
				for ( TrackletId trkId: keys ) {
					pstmt.setString(1, trkId.getNodeId());
					pstmt.setString(2, trkId.getTrackId());
					try ( ResultSet rs = pstmt.executeQuery() ) {
						if ( rs.next() ) {
							indexes.put(trkId, readIndex(rs));
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
	
	public static NodeTrackletIndex readIndex(ResultSet rs) throws SQLException {
		String node = rs.getString(1);
		String trackId = rs.getString(2);
		
		String enterZone = rs.getString(3);
		String exitZone = rs.getString(4);

		long minTs = rs.getLong(5);
		Long maxTs = rs.getLong(6);
		int partNo = rs.getInt(7);
		long minOffset = rs.getLong(8);
		Long maxOffset = rs.getLong(9);
		int count = rs.getInt(10);
		
		NodeTrackletIndex index = new NodeTrackletIndex(node, trackId, enterZone, exitZone,
														minTs, maxTs,
														partNo, minOffset, maxOffset,
														count);
		return index;
	}
	
	private static final String SQL_CREATE_TABLE
		= "create table %s ("
		+ 	"node varchar not null, "		// 1
		+ 	"track_id varchar not null, "	// 2
		+ 	"enter_zone varchar, "			// 3
		+ 	"exit_zone varchar, "			// 4
		+ 	"first_ts bigint not null, "	// 5
		+ 	"last_ts bigint not null, "		// 6
		+ 	"partition integer not null, "		// 7
		+ 	"first_offset bigint not null, "	// 8
		+ 	"last_offset bigint not null, "	// 9
		+ 	"count integer not null, "		// 10
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
