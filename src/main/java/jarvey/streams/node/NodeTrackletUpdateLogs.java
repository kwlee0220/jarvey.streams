package jarvey.streams.node;

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
import org.apache.kafka.streams.KeyValue;

import com.google.common.collect.Maps;

import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.streams.ConsumerRecordStream;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.Range;
import jarvey.streams.model.TrackletId;


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
		return streamOfIndex(getIndex(key));
	}
	
	public FStream<KeyValue<String,NodeTrack>> streamOfKeys(Iterable<TrackletId> keys) {
		return streamOfIndexes(readIndexes(keys).values());
	}
	
	public FStream<KeyValue<String,NodeTrack>> streamOfIndex(NodeTrackletIndex index) {
		TopicPartition tpart = new TopicPartition(m_topic, index.getPartitionNumber());
		ConsumerRecordStream stream 
			= ConsumerRecordStream.from(m_consumerProps)
									.addRange(tpart, index.getTopicOffsetRange())
									.build();
		
		return stream.map(rec -> KeyValue.pair(rec.key(), m_deserializer.deserialize(rec.topic(), rec.value())))
					.filter(kv -> index.getTrackletId().equals(kv.value.getTrackletId()));
	}
	
	public FStream<KeyValue<String,NodeTrack>> streamOfIndexes(Iterable<NodeTrackletIndex> indexes) {
		Set<TrackletId> keys = FStream.from(indexes).map(NodeTrackletIndex::getTrackletId).toSet();
		
		ConsumerRecordStream.Builder builder = ConsumerRecordStream.from(m_consumerProps);
		for ( NodeTrackletIndex idx: indexes ) {
			builder.addRange(new TopicPartition(m_topic, idx.getPartitionNumber()),
							idx.getTopicOffsetRange());
		}
		ConsumerRecordStream stream = builder.build();
		
		return stream.map(rec -> KeyValue.pair(rec.key(), m_deserializer.deserialize(rec.topic(), rec.value())))
					.filter(kv -> keys.contains(kv.value.getTrackletId()));
	}
	
	public List<NodeTrackletIndex> findIndexes(String whereClause) {
		String sqlString = String.format("select * from %s where %s", m_idxTableName, whereClause);
		try ( Connection conn = m_jdbc.connect() ) {
			return m_jdbc.streamQuery(sqlString)
							.mapOrThrow(this::readIndex)
							.toList();
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
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
	
	private NodeTrackletIndex readIndex(ResultSet rs) throws SQLException {
		String node = rs.getString(1);
		String trackId = rs.getString(2);
		
		String zoneSeq = rs.getString(5);
		String overlapArea = rs.getString(6);
		String assoc = rs.getString(7);

		long minTs = rs.getLong(8);
		Long maxTs = (rs.getLong(9) != -1) ? rs.getLong(9) : null;
		int partNo = rs.getInt(10);
		long minOffset = rs.getLong(11);
		Long maxOffset = (rs.getLong(12) != -1) ? rs.getLong(12) : null;
		int count = rs.getInt(13);
		
		NodeTrackletIndex index = new NodeTrackletIndex(node, trackId, zoneSeq,
														Range.between(minTs, maxTs),
														partNo,
														Range.between(minOffset, maxOffset),
														count);
		index.setMotionAssociation(overlapArea, assoc);
		
		return index;
	}
	
	private static final String SQL_CREATE_TABLE
		= "create table %s ("
		+ 	"node varchar not null, "		// 1
		+ 	"track_id varchar not null, "	// 2
		+ 	"enter_zone varchar, "			// 3
		+ 	"exit_zone varchar, "			// 4
		+ 	"zone_sequence varchar, "		// 5
		+ 	"overlap_area varchar, "		// 6
		+ 	"association varchar, "			// 7
		+ 	"first_ts bigint not null, "	// 8
		+ 	"last_ts bigint not null, "		// 9
		+ 	"partition integer not null, "		// 10
		+ 	"first_offset bigint not null, "	// 11
		+ 	"last_offset bigint not null, "	// 12
		+ 	"count integer not null, "		// 13
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
