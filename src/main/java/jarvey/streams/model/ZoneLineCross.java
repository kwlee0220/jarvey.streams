package jarvey.streams.model;

import org.locationtech.jts.geom.LineSegment;

import com.google.gson.annotations.SerializedName;

import utils.geo.util.GeoUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZoneLineCross implements Timestamped {
	public static final String STATE_UNASSIGNED = "Unassigned";
	public static final String STATE_ENTERED = "Enter";
	public static final String STATE_LEFT = "Left";
	public static final String STATE_INSIDE = "Inside";
	public static final String STATE_THROUGH = "Through";
	public static final String STATE_DELETED = "Deleted";
	public static final String[] STATES = new String[]{ STATE_UNASSIGNED, STATE_ENTERED, STATE_LEFT,
														STATE_INSIDE, STATE_THROUGH, STATE_DELETED };

	@SerializedName("node") private String m_nodeId;
	@SerializedName("luid") private long m_luid;
	@SerializedName("state") private String m_state;
	@SerializedName("line") private LineSegment m_line;
	@SerializedName("zones") private String m_zone;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	public ZoneLineCross(String nodeId, long luid, String state, String zone, LineSegment line,
							long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_luid = luid;
		m_zone = zone;
		m_state = state;
		m_line = line;
		m_frameIndex = frameIndex;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public long getLuid() {
		return m_luid;
	}
	
	public GUID getGUID() {
		return new GUID(m_nodeId, m_luid);
	}
	
	public String getZone() {
		return m_zone;
	}
	
	public GlobalZoneId getGlobalZoneId() {
		return new GlobalZoneId(m_nodeId, m_zone);
	}
	
	public String getState() {
		return m_state;
	}
	
	public LineSegment getLine() {
		return m_line;
	}
	
	public boolean isUnassigned() {
		return m_state.equals(STATE_UNASSIGNED);
	}
	
	public boolean isEntered() {
		return m_state.equals(STATE_ENTERED);
	}
	
	public boolean isLeft() {
		return m_state.equals(STATE_LEFT);
	}
	
	public boolean isInside() {
		return m_state.equals(STATE_INSIDE);
	}
	
	public boolean isThrough() {
		return m_state.equals(STATE_THROUGH);
	}
	
	public boolean isDeleted() {
		return m_state.equals(STATE_DELETED);
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public String toString() {
		if ( !isDeleted() ) {
			String zoneStr = (m_zone != null) ? String.format(", zones=%s", m_zone) : "";
			String lineStr = (m_line != null) ? String.format(", line=%s", GeoUtils.toString(m_line, 1)) : "";
			return String.format("%s[node=%s, luid=%d%s%s, frame_idx=%d, ts=%d]",
								m_state, m_nodeId, m_luid, zoneStr, lineStr, m_frameIndex, m_ts);
		}
		else {
			return String.format("%s[node=%s, luid=%d, frame_idx=%d, ts=%d]",
					m_state, m_nodeId, m_luid, m_frameIndex, m_ts);
		}
	}
}