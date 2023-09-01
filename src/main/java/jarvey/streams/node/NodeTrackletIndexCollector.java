package jarvey.streams.node;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.jdbc.JdbcProcessor;

import jarvey.streams.MockKeyValueStore;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackletId;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class NodeTrackletIndexCollector implements ValueTransformer<NodeTrack,
																Iterable<NodeTrackletIndex>> {
	private static final Logger s_logger = LoggerFactory.getLogger(NodeTrackletIndexCollector.class);

	private ProcessorContext m_context;
	private final String m_storeName;
	private final Duration m_timeToLive;
	private final boolean m_useMockStore;

	private JdbcProcessor m_jdbc;
	private String m_tableName;

	private KeyValueStore<String, NodeTrackletIndex> m_store;
	private Cancellable m_schedule;
	private Duration m_scheduleInterval = Duration.ofMinutes(1);

	private int m_hitCount = 0;

	public NodeTrackletIndexCollector(String storeName, Duration timeToLive, boolean useMockStore,
							JdbcProcessor jdbc, String tableName) {
		m_storeName = storeName;
		m_timeToLive = timeToLive;
		m_useMockStore = useMockStore;
		
		m_jdbc = jdbc;
		m_tableName = tableName;

		if ( m_useMockStore ) {
			m_store = new MockKeyValueStore<>(storeName, Serdes.String(), JarveySerdes.NodeTrackletIndex());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		m_context = context;

		if ( !m_useMockStore ) {
			m_store = (KeyValueStore<String, NodeTrackletIndex>) context.getStateStore(m_storeName);
		}
		m_schedule = context.schedule(m_scheduleInterval, PunctuationType.WALL_CLOCK_TIME,
										this::handleOldTracklet);
	}

	@Override
	public void close() {
		m_schedule.cancel();
	}

	@Override
	public Iterable<NodeTrackletIndex> transform(NodeTrack track) {
		String key = track.getKey();
		NodeTrackletIndex index = m_store.get(key);
		if ( index == null ) { // 주어진 key에 대한 첫번째 갱신인 경우.
			index = new NodeTrackletIndex(track.getTrackletId(), track.getTimestamp(),
					 						m_context.partition(), m_context.offset(), 1);
			insertIndex(track.getTrackletId(), track.getTimestamp(), m_context.partition(), m_context.offset());
			m_store.put(key, index);
		}
		else {
			index.incrementCount();
			m_store.put(key, index);
		}

		if ( track.isLastUpdate() ) {
			index.setLastUpdate(track.getZoneSequence(), track.getTimestamp(), m_context.offset());
			updateIndex(index);
			
			m_store.delete(key);
		}

		purgeOldTracklets(track.getTimestamp());
		m_hitCount = 0;

		return Collections.emptyList();
	}

	public void handleOldTracklet(long ts) {
		Duration elapsed = Duration.ofMillis(m_scheduleInterval.toMillis() * m_hitCount);
		if ( elapsed.compareTo(m_timeToLive) > 0 ) {
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("wall-clock elapsed: {} -> clear store (entries={})", elapsed,
						m_store.approximateNumEntries());
			}

			// KeyStore에 등록된 모든 index를 제거한다.
			try (KeyValueIterator<String, NodeTrackletIndex> iter = m_store.all()) {
				while ( iter.hasNext() ) {
					iter.next();
					iter.remove();
				}
			}
		}
		else if ( !elapsed.isZero() && s_logger.isInfoEnabled() ) {
			s_logger.info("wall-clock elapsed: {}", elapsed);
		}

		++m_hitCount;
	}

	private List<String> purgeOldTracklets(long ts) {
		List<String> purgeds = Lists.newArrayList();
		try (KeyValueIterator<String, NodeTrackletIndex> iter = m_store.all()) {
			while ( iter.hasNext() ) {
				KeyValue<String, NodeTrackletIndex> kv = iter.next();

				Duration maxElapsed = Duration.ofMillis(ts - kv.value.getTimestampRange().min());
				if ( maxElapsed.compareTo(m_timeToLive) > 0 ) {
					purgeds.add(kv.key);
					iter.remove();
				}
			}
		}

		return purgeds;
	}
	
//	+ 	"node varchar not null, "		// 1
//	+ 	"track_id varchar not null, "	// 2
//	+ 	"enter_zone varchar, "			// 3
//	+ 	"exit_zone varchar, "			// 4
//	+ 	"zone_sequence varchar, "		// 5
//	+ 	"overlap_area varchar, "		// 6
//	+ 	"association varchar, "			// 7
//	+ 	"first_ts bigint not null, "	// 8
//	+ 	"last_ts bigint not null, "		// 9
//	+ 	"partition integer not null, "		// 10
//	+ 	"first_offset bigint not null, "	// 11
//	+ 	"last_offset bigint not null, "	// 12
//	+ 	"count integer not null, "		// 13

	private static final String SQL_INSERT_INDEX
		= "insert into %s (node, track_id, first_ts, last_ts, partition, first_offset, last_offset, count) "
			+	"values (?, ?, ?, -1, ?, ?, -1, 1) "
			+  	"on conflict do nothing";
	private void insertIndex(TrackletId trkId, long firstTs, int partNo, long firstOffset) {
		String sqlStr = String.format(SQL_INSERT_INDEX, m_tableName);
		try ( Connection conn = m_jdbc.connect() ) {
			try ( PreparedStatement pstmt = conn.prepareStatement(sqlStr) ) {
				pstmt.setString(1, trkId.getNodeId());
				pstmt.setString(2, trkId.getTrackId());
				pstmt.setLong(3, firstTs);
				pstmt.setInt(4, partNo);
				pstmt.setLong(5, firstOffset);
				
				pstmt.execute();
			}
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}

	private static final String SQL_UPDATE_ASSOCIATION
		= "update %s set overlap_area=?, association=? where node=? and track_id=?";
	private void updateAssociation(TrackletId trkId, String overlapArea, String association) {
		String sqlStr = String.format(SQL_UPDATE_ASSOCIATION, m_tableName);
		try ( Connection conn = m_jdbc.connect() ) {
			try ( PreparedStatement pstmt = conn.prepareStatement(sqlStr) ) {
				pstmt.setString(1, overlapArea);
				pstmt.setString(2, association);
				pstmt.setString(3, trkId.getNodeId());
				pstmt.setString(4, trkId.getTrackId());
				
				pstmt.execute();
			}
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}
	
	private static final String SQL_UPDATE_INDEX
			= "update %s set enter_zone=?, exit_zone=?, zone_sequence=?, overlap_area=?, association=?, "
			+ "last_ts=?, last_offset=?, count=? where node=? and track_id=?";
	private void updateIndex(NodeTrackletIndex index) {
		String zoneSeq = index.getZoneSequence();
		String enterZone = null;
		String exitZone = null;
		if ( zoneSeq != null ) {
			int len = zoneSeq.length();
			enterZone = zoneSeq.substring(1, 2);
			exitZone = zoneSeq.substring(len-2, len-1);
		}
		
		String sqlStr = String.format(SQL_UPDATE_INDEX, m_tableName);
		try ( Connection conn = m_jdbc.connect() ) {
			try ( PreparedStatement pstmt = conn.prepareStatement(sqlStr) ) {
				pstmt.setString(1, enterZone);
				pstmt.setString(2, exitZone);
				pstmt.setString(3, index.getZoneSequence());
				pstmt.setString(4, index.getOverlapAreaId());
				pstmt.setString(5, index.getAssociation());
				pstmt.setLong(6, index.getTimestampRange().max());
				pstmt.setLong(7, index.getTopicOffsetRange().max());
				pstmt.setInt(8, index.getUpdateCount());
				pstmt.setString(9, index.getNodeId());
				pstmt.setString(10, index.getTrackId());
				
				pstmt.execute();
			}
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}
}
