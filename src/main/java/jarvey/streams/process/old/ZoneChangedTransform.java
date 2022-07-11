/**
 * 
 */
package jarvey.streams.process.old;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapper;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import jarvey.streams.model.GUID;
import jarvey.streams.model.old.ZoneChangedDAO;
import jarvey.streams.model.old.ZonedTrackDAO;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ZoneChangedTransform implements ValueMapper<ZonedTrackDAO, Iterable<ZoneChangedDAO>> {
	private final Map<GUID,Set<String>> m_zones = Maps.newHashMap();
	
	private static final Set<String> EMPTY_ZONES = Collections.emptySet();

	@Override
	public Iterable<ZoneChangedDAO> apply(ZonedTrackDAO ev) {
		GUID guid = new GUID(ev.getNodeId(), ev.getLuid());

		Set<String> prevZones = m_zones.computeIfAbsent(guid, k -> Sets.newHashSet());
		if ( ev.getState().equals(ZonedTrackDAO.State.DELETED) ) {
			ZoneChangedDAO deleted
				= new ZoneChangedDAO(ev.getNodeId(), ev.getLuid(),
									EMPTY_ZONES, EMPTY_ZONES, prevZones, ZoneChangedDAO.State.DELETED,
									ev.getFrameIndex(), ev.getTimestamp());
			return Arrays.asList(deleted);
		}
		
		Set<String> zones = ev.getZones() == null ? EMPTY_ZONES : ev.getZones();
		if ( !prevZones.equals(zones) ) {
			Set<String> entereds = Sets.difference(zones, prevZones);
			Set<String> lefts = Sets.difference(prevZones, zones);
			m_zones.put(guid, zones);
		
			ZoneChangedDAO changed = new ZoneChangedDAO(ev.getNodeId(), ev.getLuid(),
														zones, entereds, lefts,
														ZoneChangedDAO.State.CHANGED,
														ev.getFrameIndex(), ev.getTimestamp());
			return Arrays.asList(changed);
		}
		else {
			return Collections.emptyList();
		}
	}
}
