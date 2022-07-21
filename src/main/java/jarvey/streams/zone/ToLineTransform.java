/**
 * 
 */
package jarvey.streams.zone;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.streams.UpdateTimeAssociatedKeyValue;
import jarvey.streams.model.GUID;
import jarvey.streams.model.ObjectTrack;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToLineTransform implements ValueTransformerWithKey<String, ObjectTrack, Iterable<LineTrack>> {
	private static final Logger s_logger = LoggerFactory.getLogger(ToLineTransform.class);

	private static final Duration DEFAULT_TTL_MINUTES = Duration.ofMinutes(5);
	private static final int DEFAULT_CHECKUP_MINUTES = 3;

	private UpdateTimeAssociatedKeyValue<GUID, ObjectTrack> m_store;
	
	public ToLineTransform() {}

	@Override
	public void init(ProcessorContext context) {
		KeyValueStore<GUID, ObjectTrack> store = context.getStateStore("last-tracks");
		m_store = UpdateTimeAssociatedKeyValue.of(store);
		context.schedule(Duration.ofMinutes(DEFAULT_CHECKUP_MINUTES),
						PunctuationType.WALL_CLOCK_TIME,
						ts -> m_store.deleteOldEntries(DEFAULT_TTL_MINUTES));
	}

	@Override
	public Iterable<LineTrack> transform(String key, ObjectTrack track) {
		GUID guid = track.getGuid();
		if ( track.isDeleted() ) {
			// 'Delete' event의 경우에는 출력 event를 생성하지 않음.
			s_logger.debug("delete: guid={}", guid);
			m_store.delete(guid);
			
			LineTrack line = new LineTrack(track.getNodeId(), track.getLuid(), LineTrack.STATE_DELETED,
											null, null, track.getFrameIndex(), track.getTimestamp());
			return Arrays.asList(line);
		}
		else {
			ObjectTrack lastTrack = m_store.get(guid);
			m_store.put(guid, track);
			
			if ( lastTrack != null ) {
				LineTrack vector = LineTrack.from(lastTrack, track);
				return Arrays.asList(vector);
			}
			else {
				// GUID를 기준으로 첫번재 event인 경우는 LineTrack 객체를 생성할 수 없어
				// 출력 event를 생성하지 않음.
				return Collections.emptyList();
			}
		}
	}

	@Override
	public void close() { }
}
