package jarvey.streams.assoc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import utils.LocalDateTimes;
import utils.geo.util.CoordinateTransform;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.assoc.motion.OverlapAreaRegistry;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class GlobalTrackExporter extends AbstractGlobalTrackGenerator
										implements KafkaConsumerRecordProcessor<String,byte[]> {
	private static final Logger s_logger = LoggerFactory.getLogger(GlobalTrackExporter.class);
											
	private final AssociationStore m_assocStore;
	private final JdbcProcessor m_exportJdbc;
	private final Duration m_runningTime;
	
	private long m_endTs = -1;
	private final Deserializer<NodeTrack> m_deser;
	private final AssociationCache m_cache = new AssociationCache(64);
	private final Set<String> m_runningTrajs = Sets.newHashSet();
	private final CoordinateTransform m_coordTrans;
	
	private Connection m_conn;
	private Statement m_stmt;
	
	public GlobalTrackExporter(OverlapAreaRegistry areaRegistry, AssociationStore assocStore,
								JdbcProcessor exportJdbc, Duration runningTime) throws SQLException {
		super(areaRegistry);
		
		m_assocStore = assocStore;
		m_exportJdbc = exportJdbc;
		m_runningTime = runningTime;
		
		m_deser = JarveySerdes.NodeTrack().deserializer();
		m_coordTrans = CoordinateTransform.getTransformToWgs84(5186);
		
		m_conn = exportJdbc.connect();
		m_stmt = m_conn.createStatement();
	}

	@Override
	public void close() throws Exception {
		for ( GlobalTrack gtrack: closeGenerator() ) {
			try {
				write(gtrack);
			}
			catch ( SQLException e ) {
				s_logger.error("fails to write global tracks", e);
			}
		}
		m_stmt.close();
		m_conn.close();
	}

	@Override
	public ProcessResult process(ConsumerRecord<String, byte[]> record) {
		NodeTrack track = m_deser.deserialize(record.topic(), record.value());
		
		long ts = track.getTimestamp();
		if ( m_endTs < 0 ) {
			m_endTs = ts + m_runningTime.toMillis();
		}
		else if ( ts >= m_endTs ) {
			return new ProcessResult(record.offset(), false);
		}
		
		for ( GlobalTrack gtrack: generate(track) ) {
			try {
				write(gtrack);
			}
			catch ( SQLException e ) {
				s_logger.error("fails to write global tracks", e);
			}
		}
		
		return new ProcessResult(record.offset() + 1);
	}

	@Override
	public void timeElapsed(long expectedTs) {
		for ( GlobalTrack gtrack: handleTimeElapsed(expectedTs) ) {
			try {
				write(gtrack);
			}
			catch ( SQLException e ) {
				s_logger.error("fails to write global tracks", e);
			}
		}
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		return m_deser.deserialize(record.topic(), record.value()).getTimestamp();
	}
	
	@Override
	protected Association findAssociation(LocalTrack ltrack) {
		return m_cache.get(ltrack.getTrackletId())
						.getOrElse(() -> {
							try {
								Association assoc = m_assocStore.getAssociation(ltrack.getTrackletId());
								m_cache.put(ltrack.getTrackletId(), assoc);
								return assoc;
							}
							catch ( SQLException e ) {
								return null;
							}
						});
	}
	
	private void write(GlobalTrack gtrack) throws SQLException {
		if ( gtrack.isDeleted() ) {
			m_runningTrajs.remove(gtrack.getKey());
			return;
		}
		
		LocalDateTime ldt = LocalDateTimes.fromEpochMillis(gtrack.getTimestamp());
		String tsStr = ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
		
		Coordinate lonlat = m_coordTrans.transform(gtrack.getLocation().getCoordinate());
		String wktStr = String.format("ST_IMPORTFROMWKT('MPOINT(%s, %.8f, %.8f)')", tsStr, lonlat.x, lonlat.y);
		
		String sql = null;
		if ( !m_runningTrajs.contains(gtrack.getKey()) ) {
			sql = String.format("insert into cctv_etri(id, mo) values ('%s', %s)", gtrack.getKey(), wktStr);
			try {
				m_stmt.execute(sql);
			}
			catch ( SQLException e ) {
				m_stmt.execute(String.format("delete from cctv_etri where id='%s'", gtrack.getKey()));
				m_stmt.execute(sql);
			}
			m_runningTrajs.add(gtrack.getKey());
		}
		else {
			sql = String.format("update cctv_etri set mo=%s where id='%s'", wktStr, gtrack.getKey());
			m_stmt.execute(sql);
		}
//		System.out.println(sql);
	}
}
