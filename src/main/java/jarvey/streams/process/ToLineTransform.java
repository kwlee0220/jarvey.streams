/**
 * 
 */
package jarvey.streams.process;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.streams.kstream.ValueMapper;

import com.google.common.collect.Maps;

import jarvey.streams.model.GUID;
import jarvey.streams.model.LineTrack;
import jarvey.streams.model.ObjectTrack;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ToLineTransform implements ValueMapper<ObjectTrack, Iterable<LineTrack>> {
	private final Map<GUID, ObjectTrack> m_lastTracks = Maps.newHashMap();

	@Override
	public Iterable<LineTrack> apply(ObjectTrack ev) {
		GUID guid = ev.getGuid();
		ObjectTrack lastTrack = m_lastTracks.get(guid);
		m_lastTracks.put(guid, ev);
		
		if ( lastTrack == null ) {
			return Collections.emptyList();
		}
		
		LineTrack vector = LineTrack.from(lastTrack, ev);
		return Arrays.asList(vector);
	}
}
