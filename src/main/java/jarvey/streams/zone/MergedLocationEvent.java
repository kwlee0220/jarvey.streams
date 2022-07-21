/**
 * 
 */
package jarvey.streams.zone;

import java.util.Arrays;
import java.util.List;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MergedLocationEvent {
	private final List<ZoneLineRelationEvent> m_zoneLineCrosses;
	private final LocationChanged m_locationChanged;
	
	public static List<MergedLocationEvent> from(List<ZoneLineRelationEvent> crosses, LocationChanged locationChanged) {
		return Arrays.asList(new MergedLocationEvent(crosses, null),
							new MergedLocationEvent(null, locationChanged));
	}
	
	public static List<MergedLocationEvent> from(ZoneLineRelationEvent cross, LocationChanged locationChanged) {
		return from(Arrays.asList(cross), locationChanged);
	}
	
	public static List<MergedLocationEvent> from(List<ZoneLineRelationEvent> crosses) {
		return Arrays.asList(new MergedLocationEvent(crosses, null));
	}
	
	public static List<MergedLocationEvent> from(ZoneLineRelationEvent cross) {
		return from(Arrays.asList(cross));
	}
	
	private MergedLocationEvent(List<ZoneLineRelationEvent> crossed, LocationChanged locationChanged) {
		m_zoneLineCrosses = crossed;
		m_locationChanged = locationChanged;
	}
	
	public boolean isZoneLineCrosses() {
		return m_zoneLineCrosses != null;
	}
	
	public boolean isLocationChanged() {
		return m_locationChanged != null;
	}
	
	public List<ZoneLineRelationEvent> getZoneLineCrosses() {
		return m_zoneLineCrosses;
	}
	
	public LocationChanged getLocationChanged() {
		return m_locationChanged;
	}
}
