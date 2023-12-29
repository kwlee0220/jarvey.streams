package jarvey.streams.assoc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import utils.LocalDateTimes;
import utils.geo.util.CoordinateTransform;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;
import jarvey.streams.serialization.json.GsonUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildGlobalTrajectories implements KafkaConsumerRecordProcessor<String,byte[]> {
	private static final Logger s_logger = LoggerFactory.getLogger(BuildGlobalTrajectories.class);
	
	private final Deserializer<GlobalTrack> m_deser;
	private final Set<String> m_runningTrajs = Sets.newHashSet();	// insert / update 를 구분하기 위함.
	private final CoordinateTransform m_coordTrans;
	
	private final Connection m_conn;
	private final Statement m_stmt;
	
	public BuildGlobalTrajectories(JdbcProcessor exportJdbc) throws SQLException {
		m_deser = GsonUtils.getSerde(GlobalTrack.class).deserializer();
		m_coordTrans = CoordinateTransform.getTransformToWgs84(5186);
		
		m_conn = exportJdbc.connect();
		m_stmt = m_conn.createStatement();
	}

	@Override
	public void close() throws Exception {
		m_stmt.close();
		m_conn.close();
	}

	@Override
	public ProcessResult process(TopicPartition tpart, ConsumerRecord<String, byte[]> record) {
		GlobalTrack gtrack = m_deser.deserialize(record.topic(), record.value());
		
		try {
			write(gtrack);
			return ProcessResult.of(tpart, record.offset() + 1, false);
		}
		catch ( SQLException e ) {
			s_logger.error("fails to write global tracks", e);
			return ProcessResult.NULL;
		}
	}

	@Override
	public ProcessResult timeElapsed(TopicPartition tpart, long expectedTs) {
		return ProcessResult.NULL;
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		return m_deser.deserialize(record.topic(), record.value()).getTimestamp();
	}
	
	private void write(GlobalTrack gtrack) throws SQLException {
		// delete 이벤트에 해당하는 global track은 database에 추가하지 않는다.
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
			// tracklet의 첫번째 track인 경우에는 레코드를 추가한다.
			// 만일 동일 식별자의 레코드가 존재하는 경우에는 기존 것을 삭제하고 본 레코드를 추가한다.
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
