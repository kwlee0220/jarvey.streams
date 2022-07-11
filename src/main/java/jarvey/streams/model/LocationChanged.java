package jarvey.streams.model;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class LocationChanged {
	@SerializedName("node") private String m_nodeId;
	@SerializedName("luid") private long m_luid;
	@SerializedName("zones") private Set<String> m_zoneIds = Sets.newHashSet();
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	@SerializedName("deleted") private boolean m_deleted = false;
	
	public static LocationChanged from(GUID guid, ZoneLocations zones) {
		return new LocationChanged(guid.getNodeId(), guid.getLuid(), zones.getZoneIds(), zones.isDeleted(),
									zones.getFrameIndex(), zones.getTimestamp());
	}
	
	public LocationChanged(String nodeId, long luid, Set<String> zoneIds, boolean deleted,
							long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_luid = luid;
		m_zoneIds = zoneIds;
		m_deleted = deleted;
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
	
	public boolean isDeleted() {
		return m_deleted;
	}
	
	@Override
	public String toString() {
		if ( !m_deleted ) {
			return String.format("%s[node=%s, luid=%d, zones=%s, frame_idx=%d, ts=%d]",
								getClass().getSimpleName(), m_nodeId, m_luid, m_zoneIds, m_frameIndex, m_ts);
		}
		else {
			return String.format("Deleted[node=%s, luid=%d, frame_idx=%d, ts=%d]",
									m_nodeId, m_luid, m_frameIndex, m_ts);
		}
	}
}