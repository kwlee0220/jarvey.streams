package jarvey.streams.zone;

import java.util.Set;

import com.google.gson.annotations.SerializedName;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZoneLocations {
	@SerializedName("zones") private final Set<String> m_zoneIds;
	@SerializedName("frame_index") private final long m_frameIndex;
	@SerializedName("ts") private final long m_ts;
	
	public ZoneLocations(Set<String> zoneIds, long frameIndex, long ts) {
		m_zoneIds = zoneIds;
		m_frameIndex = frameIndex;
		m_ts = ts;
	}
	
	public Set<String> getZoneIds() {
		return m_zoneIds;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public String toString() {
		return String.format("%s[zones=%s, frame_idx=%d, ts=%d]",
				getClass().getSimpleName(),m_zoneIds, m_frameIndex, m_ts);
	}
}