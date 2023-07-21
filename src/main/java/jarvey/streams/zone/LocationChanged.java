package jarvey.streams.zone;

import java.util.Set;

import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.TrackletId;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class LocationChanged {
	@SerializedName("node") private final String m_nodeId;
	@SerializedName("luid") private final String m_trackId;
	@SerializedName("zones") private final Set<String> m_zoneIds;
	@SerializedName("frame_index") private final long m_frameIndex;
	@SerializedName("ts") private final long m_ts;
	
	public static LocationChanged from(TrackletId guid, ZoneLocations zones) {
		return new LocationChanged(guid.getNodeId(), guid.getTrackId(), zones.getZoneIds(),
									zones.getFrameIndex(), zones.getTimestamp());
	}
	
	private LocationChanged(String nodeId, String trackId, Set<String> zoneIds, long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_trackId = trackId;
		m_zoneIds = zoneIds;
		m_frameIndex = frameIndex;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public String getTrackId() {
		return m_trackId;
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
				getClass().getSimpleName(), m_nodeId, m_trackId, m_zoneIds, m_frameIndex, m_ts);
	}
}