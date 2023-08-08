package jarvey.streams.zone;

import java.util.Set;

import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.Timestamped;
import utils.func.Funcs;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Residents implements Timestamped {
	@SerializedName("zone") private String m_zoneId;
	@SerializedName("tracks") private Set<String> m_trackIds;
	@SerializedName("ts") private long m_ts;
	
	public Residents(String zoneId, Set<String> trackIds, long ts) {
		m_zoneId = zoneId;
		m_trackIds = trackIds;
		m_ts = ts;
	}
	
	public String getZoneId() {
		return m_zoneId;
	}
	
	public Set<String> getTrackIds() {
		return m_trackIds;
	}
	
	@Override
	public long getTimestamp() {
		return m_ts;
	}
	
	public boolean contains(String trackId) {
		return m_trackIds.contains(trackId);
	}
	
	public Residents remove(String trackId, long ts) {
		Set<String> removed = Funcs.removeCopy(m_trackIds, trackId);
		return new Residents(m_zoneId, removed, ts);
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s%s, ts=%d]",
							getClass().getSimpleName(), m_zoneId, m_trackIds, m_ts);
	}
}