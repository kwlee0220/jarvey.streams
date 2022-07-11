package jarvey.streams.model;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

import com.google.gson.annotations.SerializedName;

import utils.geo.util.GeoUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ObjectTrack implements Timestamped {
	public static final String STATE_NULL = "NULL";
	public static final String STATE_TENTATIVE = "Tentative";
	public static final String STATE_CONFIRMED = "Confirmed";
	public static final String STATE_TEMPORARILY_LOST = "TemporarilyLost";
	public static final String STATE_DELETED = "Deleted";
	public static final String[] STATES = new String[]{ STATE_NULL, STATE_TENTATIVE, STATE_CONFIRMED,
														STATE_TEMPORARILY_LOST, STATE_DELETED };

	@SerializedName("node") private String m_nodeId;
	@SerializedName("luid") private long m_luid;
	@SerializedName("state") private String m_state;
	@SerializedName("location") private Envelope m_box;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	@SerializedName("world_coord") private Point m_worldCoords;
	@SerializedName("distance") private double m_distance;
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public long getLuid() {
		return m_luid;
	}
	
	public GUID getGuid() {
		return new GUID(m_nodeId, m_luid);
	}
	
	public String getState() {
		return m_state;
	}
	
	public boolean isDeleted() {
		return m_state.equals(STATE_DELETED);
	}
	
	public Envelope getBox() {
		return m_box;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public Point getWorldCoordinates() {
		return m_worldCoords;
	}
	
	public double getDistance() {
		return m_distance;
	}
	
	@Override
	public String toString() {
		String trackStr = (m_state.equals(STATE_DELETED)) ? "Deleted" : "Tracked";
		return String.format("%s[node=%s, luid=%d, box=%s, world=%s, frame_idx=%d, ts=%d]",
								trackStr, m_nodeId, m_luid, GeoUtils.toString(m_box, 0),
								GeoUtils.toString(m_worldCoords, 1), m_frameIndex, m_ts);
	}
}