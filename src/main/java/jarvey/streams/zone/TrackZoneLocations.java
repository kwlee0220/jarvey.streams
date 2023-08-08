package jarvey.streams.zone;

import java.util.Collections;
import java.util.Set;

import com.google.gson.annotations.SerializedName;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TrackZoneLocations {
	private static final Set<String> EMPTY_ZONES = Collections.emptySet();
	
	@SerializedName("track") private final String m_trackId;
	@SerializedName("zones") private final Set<String> m_zoneIds;
	@SerializedName("ts") private long m_ts;
	
	public static TrackZoneLocations empty(String trackId, long ts) {
		return new TrackZoneLocations(trackId, EMPTY_ZONES, ts);
	}
	
	public TrackZoneLocations(String trackId, Set<String> zoneIds, long ts) {
		m_trackId = trackId;
		m_zoneIds = zoneIds;
		m_ts = ts;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	public Set<String> getZoneIds() {
		return m_zoneIds;
	}
	
	public boolean contains(String zoneId) {
		return m_zoneIds.contains(zoneId);
	}
	
	public int size() {
		return m_zoneIds.size();
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public String toString() {
		return String.format("%s@%s#%d", m_trackId, m_zoneIds, m_ts);
	}
}