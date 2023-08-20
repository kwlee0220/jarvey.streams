package jarvey.streams.zone;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import jarvey.streams.model.ObjectTrack;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ToLineTransform<T extends ObjectTrack> implements ValueTransformer<T, Iterable<LineTrack>> {
	private static final Logger s_logger = LoggerFactory.getLogger(ToLineTransform.class);

	private static final Duration DEFAULT_TTL_MINUTES = Duration.ofSeconds(3);

	private final Map<String,ObjectTrack> m_lastTracks = Maps.newHashMap(); 
	private long m_ttlThreshold = DEFAULT_TTL_MINUTES.toMillis();
	
	ToLineTransform() {}

	@Override
	public void init(ProcessorContext context) { }

	@Override
	public Iterable<LineTrack> transform(T track) {
		deleteOldEntries(track.getTimestamp());
		
		String trkId = track.getKey();
		if ( track.isDeleted() ) {
			// 'Delete' event의 경우에는 출력 event를 생성하지 않음.
			s_logger.debug("delete: tracklet={}", trkId);
			m_lastTracks.remove(trkId);
			
			LineTrack line = LineTrack.DELETED(track);
			return Arrays.asList(line);
		}
		else {
			ObjectTrack lastTrack = m_lastTracks.get(trkId);
			m_lastTracks.put(trkId, track);
			
			if ( lastTrack != null ) {
				LineTrack vector = LineTrack.from(lastTrack, track);
				return Arrays.asList(vector);
			}
			else {
				// GUID를 기준으로 첫번재 event인 경우는 point 정보만 갖는 LineTrack 객체를 생성한다
				return Arrays.asList(LineTrack.from(track));
			}
		}
	}

	@Override
	public void close() { }
	
	private void deleteOldEntries(long now) {
		List<ObjectTrack> oldTracks =  FStream.from(m_lastTracks.values())
												.filter(t -> (now - t.getTimestamp()) >= m_ttlThreshold)
												.toList();
		for ( ObjectTrack track: oldTracks ) {
			if ( s_logger.isInfoEnabled() ) {
				Duration age = Duration.ofMillis(now - track.getTimestamp());
				s_logger.info("purge too old track-cluster: {}, age={}", track, age);
			}
			oldTracks.remove(track);
		}
	}
}
