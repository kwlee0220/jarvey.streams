/**
 * 
 */
package jarvey.streams.process.old;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import jarvey.streams.model.GlobalZoneId;
import jarvey.streams.model.ResidentChanged;
import jarvey.streams.model.old.ZoneChangedDAO;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ResidentsChangedTransformOld implements ValueMapper<ZoneChangedDAO, Iterable<ResidentChanged>> {
	private final Map<GlobalZoneId,Set<Long>> m_residents = Maps.newHashMap();
	
	@Override
	public Iterable<ResidentChanged> apply(ZoneChangedDAO ev) {
		List<ResidentChanged> changeEvents = Lists.newArrayList();
		
		if ( ev.getState().equals(ZoneChangedDAO.State.CHANGED) ) {
			for ( String zone: ev.getEnteredZones() ) {
				Set<Long> residents = m_residents.computeIfAbsent(new GlobalZoneId(ev.getNodeId(), zone),
																k -> Sets.newHashSet());
				if ( residents.add(ev.getLuid()) ) {
					changeEvents.add(new ResidentChanged(ev.getNodeId(), zone, residents,
															ev.getFrameIndex(), ev.getTimestamp()));
				}
			}
			for ( String zone: ev.getLeftZones() ) {
				Set<Long> residents = m_residents.computeIfAbsent(new GlobalZoneId(ev.getNodeId(), zone),
																k -> Sets.newHashSet());
				if ( residents.remove(ev.getLuid()) ) {
					changeEvents.add(new ResidentChanged(ev.getNodeId(), zone, residents,
															ev.getFrameIndex(), ev.getTimestamp()));
				}
			}
		}
		else {
			for ( String zone: ev.getLeftZones() ) {
				GlobalZoneId gzone = new GlobalZoneId(ev.getNodeId(), zone);
				Set<Long> residents = m_residents.computeIfAbsent(gzone, k -> Sets.newHashSet());
				if ( residents.remove(ev.getLuid()) ) {
					changeEvents.add(new ResidentChanged(ev.getNodeId(), zone, residents,
															ev.getFrameIndex(), ev.getTimestamp()));
				}
			}
		}
		
		return changeEvents;
	}
}
