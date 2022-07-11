/**
 * 
 */
package jarvey.streams.process;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import jarvey.streams.model.GUID;
import jarvey.streams.model.GlobalZoneId;
import jarvey.streams.model.ResidentChanged;
import jarvey.streams.model.ZoneLineCross;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ResidentsChangedTransform implements ValueMapper<ZoneLineCross, Iterable<ResidentChanged>> {
	private final Map<GlobalZoneId,Set<Long>> m_residents = Maps.newHashMap();
	private final Map<GUID,Set<String>> m_memberships = Maps.newHashMap();
	
	@Override
	public Iterable<ResidentChanged> apply(ZoneLineCross ev) {
		GUID guid = new GUID(ev.getNodeId(), ev.getLuid());
		
		String nodeId = ev.getNodeId();
		GlobalZoneId gzone = new GlobalZoneId(nodeId, ev.getZone());
		
		Set<Long> residents;
		List<ResidentChanged> changeEvents = Lists.newArrayList();
		switch ( ev.getState() ) {
			case ZoneLineCross.STATE_UNASSIGNED:
			case ZoneLineCross.STATE_INSIDE:
			case ZoneLineCross.STATE_THROUGH:
				break;
			case ZoneLineCross.STATE_ENTERED:
				residents = m_residents.computeIfAbsent(gzone, k -> Sets.newHashSet());
				if ( residents.add(ev.getLuid()) ) {
					changeEvents.add(new ResidentChanged(ev.getNodeId(), ev.getZone(), residents,
															ev.getFrameIndex(), ev.getTimestamp()));
					m_memberships.computeIfAbsent(guid, k -> Sets.newHashSet()).add(ev.getZone());
				}
				break;
			case ZoneLineCross.STATE_LEFT:
				residents = m_residents.computeIfAbsent(gzone, k -> Sets.newHashSet());
				if ( residents.remove(ev.getLuid()) ) {
					changeEvents.add(new ResidentChanged(ev.getNodeId(), ev.getZone(), residents,
															ev.getFrameIndex(), ev.getTimestamp()));
					Set<String> membership = m_memberships.get(guid);
					if ( membership != null ) {
						if ( membership.remove(ev.getZone()) && membership.size() == 0 ) {
							m_memberships.remove(guid);
						}
					}
				}
				break;
			case ZoneLineCross.STATE_DELETED:
				Set<String> memberships = m_memberships.remove(guid);
				if ( memberships != null ) {
					for ( String zone: memberships ) {
						GlobalZoneId gz = new GlobalZoneId(ev.getNodeId(), zone);
						residents = m_residents.computeIfAbsent(gz, k -> Sets.newHashSet());
						if ( residents.remove(ev.getLuid()) ) {
							changeEvents.add(new ResidentChanged(ev.getNodeId(), zone, residents,
																	ev.getFrameIndex(), ev.getTimestamp()));
						}
					}
				}
				break;
			default:
				throw new AssertionError("unexpected state: state=" + ev.getState());
		}
		
		return changeEvents;
	}
}
