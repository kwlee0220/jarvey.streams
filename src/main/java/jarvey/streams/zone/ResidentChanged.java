package jarvey.streams.zone;

import java.util.Set;

import com.google.gson.annotations.SerializedName;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ResidentChanged {
	@SerializedName("node") private String m_nodeId;
	@SerializedName("zone") private String m_zone;
	@SerializedName("luids") private Set<Long> m_luids;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	public static ResidentChanged from(GlobalZoneId gzone, Residents residents) {
		if ( residents != null ) {
			return new ResidentChanged(gzone.getNodeId(), gzone.getZoneId(), residents.getLuids(),
										residents.getFrameIndex(), residents.getTimestamp());
		}
		else {
			return null;
		}
	}
	
	public ResidentChanged(String nodeId, String zone, Set<Long> luids, long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_zone = zone;
		m_luids = luids;
		m_frameIndex = frameIndex;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public String getZone() {
		return m_zone;
	}
	
	public Set<Long> getResidents() {
		return m_luids;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public String toString() {
		return String.format("%s[gzone=%s/%s, luids=%s, frame=%d, ts=%d]",
							getClass().getSimpleName(), m_nodeId, m_zone, m_luids, m_frameIndex, m_ts);
	}
}