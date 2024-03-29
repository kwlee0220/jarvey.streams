package jarvey.streams.updatelog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.annotations.SerializedName;

import utils.jdbc.JdbcProcessor;

import jarvey.streams.MockKeyValueStore;
import jarvey.streams.serialization.json.GsonUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KeyedUpdateIndexBuilder<T extends KeyedUpdate>
									implements FixedKeyProcessor<String, T, Iterable<KeyedUpdateIndex>> {
	private static final Logger s_logger = LoggerFactory.getLogger(KeyedUpdateIndexBuilder.class);

	private FixedKeyProcessorContext<String,Iterable<KeyedUpdateIndex>> m_context;
	private final String m_storeName;
	private final Duration m_timeToLive;
	private final boolean m_useMockStore;

	private JdbcProcessor m_jdbc;
	private String m_insertSql;	
	private String m_updateSql;	

	private KeyValueStore<String, UpdateState> m_store;
	private Cancellable m_schedule;
	private Duration m_scheduleInterval = Duration.ofMinutes(1);

	private int m_hitCount = 0;
	
	private static class UpdateState {
		@SerializedName("first_ts") private long m_firstTs;
		@SerializedName("count") private int m_count;
		
		UpdateState(long firstTs, int count) {
			m_firstTs = firstTs;
			m_count = count;
		}
		
		@Override
		public String toString() {
			return String.format("%d@%d", m_count, m_firstTs);
		}
	}

	public KeyedUpdateIndexBuilder(String storeName, Duration timeToLive, boolean useMockStore,
									JdbcProcessor jdbc, String tableName) {
		m_storeName = storeName;
		m_timeToLive = timeToLive;
		m_useMockStore = useMockStore;
		
		m_jdbc = jdbc;
		m_insertSql = String.format(SQL_INSERT_INDEX, tableName);
		m_updateSql = String.format(SQL_UPDATE_INDEX, tableName);

		if ( m_useMockStore ) {
			m_store = new MockKeyValueStore<>(storeName, Serdes.String(),
												GsonUtils.getSerde(UpdateState.class));
		}
	}

	@Override
	public void init(FixedKeyProcessorContext<String,Iterable<KeyedUpdateIndex>> context) {
		m_context = context;

		if ( !m_useMockStore ) {
			m_store = (KeyValueStore<String, UpdateState>)context.getStateStore(m_storeName);
		}
		m_schedule = context.schedule(m_scheduleInterval, PunctuationType.WALL_CLOCK_TIME,
										this::handleOldTracklet);
	}

	@Override
	public void close() {
		m_schedule.cancel();
	}

	@Override
	public void process(FixedKeyRecord<String, T> updateRecord) {
		RecordMetadata meta = m_context.recordMetadata().get();
		T update = updateRecord.value();
		String key = update.getKey();
		
		UpdateState record = m_store.get(key);
		if ( record == null ) { // 주어진 key에 대한 첫번째 갱신인 경우.
			// 첫번째 갱신이 마지막(delete) 이벤트인 경우는 무시한다.
			if ( update.isLastUpdate() ) {
				return;
			}

			if ( s_logger.isDebugEnabled() ) {
				s_logger.debug("create an index entry: key={}, ts={}, offset={}",
								key, update.getTimestamp(), meta.offset());
			}
			
			insertIndex(key, meta.partition(), meta.offset(), update.getTimestamp());
			record = new UpdateState(update.getTimestamp(), 1);
			m_store.put(key, record);
		}
		else {
			record.m_count += 1;
			m_store.put(key, record);
		}

		if ( update.isLastUpdate() ) {
			if ( s_logger.isDebugEnabled() ) {
				s_logger.debug("finalize index entry: key={}, ts={}, offset={}, count={}",
								key, update.getTimestamp(), meta.offset(), record.m_count);
			}
			
			updateIndex(key, meta.offset(), update.getTimestamp(), record.m_count);
			m_store.delete(key);
		}

		purgeOldTracklets(update.getTimestamp());
		m_hitCount = 0;
	}

	public void handleOldTracklet(long ts) {
		Duration elapsed = Duration.ofMillis(m_scheduleInterval.toMillis() * m_hitCount);
		if ( elapsed.compareTo(m_timeToLive) > 0 ) {
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("wall-clock elapsed: {} -> clear store (entries={})", elapsed,
						m_store.approximateNumEntries());
			}

			// KeyStore에 등록된 모든 index를 제거한다.
			try (KeyValueIterator<String, UpdateState> iter = m_store.all()) {
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
		try (KeyValueIterator<String, UpdateState> iter = m_store.all()) {
			while ( iter.hasNext() ) {
				KeyValue<String, UpdateState> kv = iter.next();

				Duration maxElapsed = Duration.ofMillis(ts - kv.value.m_firstTs);
				if ( maxElapsed.compareTo(m_timeToLive) > 0 ) {
					purgeds.add(kv.key);
					iter.remove();
				}
			}
		}

		return purgeds;
	}

	private static final String SQL_INSERT_INDEX
		= "insert into %s (key, partition, first_offset, last_offset, first_ts, last_ts, count) "
			+	"values (?, ?, ?, -1, ?, -1, 1) "
		+ "on conflict (key) "
		+ "do update set "
			+ "partition=?, "
			+ "first_offset=?, last_offset=-1, "
			+ "first_ts=?, last_ts=-1, "
			+ "count=1";
	private void insertIndex(String key, int partNo, long firstOffset, long firstTs) {
		try ( Connection conn = m_jdbc.connect() ) {
			try ( PreparedStatement pstmt = conn.prepareStatement(m_insertSql) ) {
				pstmt.setString(1, key);
				pstmt.setInt(2, partNo);
				pstmt.setLong(3, firstOffset);
				pstmt.setLong(4, firstTs);

				pstmt.setInt(5, partNo);
				pstmt.setLong(6, firstOffset);
				pstmt.setLong(7, firstTs);
				
				pstmt.execute();
			}
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}
	
	private static final String SQL_UPDATE_INDEX = "update %s set last_offset=?, last_ts=?, count=? where key=?";
	private void updateIndex(String key, long lastOffset, long lastTs, int count) {
		try ( Connection conn = m_jdbc.connect() ) {
			try ( PreparedStatement pstmt = conn.prepareStatement(m_updateSql) ) {
				pstmt.setLong(1, lastOffset);
				pstmt.setLong(2, lastTs);
				pstmt.setInt(3, count);
				pstmt.setString(4, key);
				
				pstmt.execute();
			}
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
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
