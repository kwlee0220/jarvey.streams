package jarvey.streams.zone;

import java.util.Set;

import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.GUID;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class LocationChanged {
	@SerializedName("node") private final String m_nodeId;
	@SerializedName("luid") private final long m_luid;
	@SerializedName("zones") private final Set<String> m_zoneIds;
	@SerializedName("frame_index") private final long m_frameIndex;
	@SerializedName("ts") private final long m_ts;
	
	public static LocationChanged from(GUID guid, ZoneLocations zones) {
		return new LocationChanged(guid.getNodeId(), guid.getLuid(), zones.getZoneIds(),
									zones.getFrameIndex(), zones.getTimestamp());
	}
	
	private LocationChanged(String nodeId, long luid, Set<String> zoneIds, long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_luid = luid;
		m_zoneIds = zoneIds;
		m_frameIndex = frameIndex;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public long getLuid() {
		return m_luid;
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
		return String.format("%s[node=%s, luid=%d, zones=%s, frame_idx=%d, ts=%d]",
				getClass().getSimpleName(), m_nodeId, m_luid, m_zoneIds, m_frameIndex, m_ts);
	}
}