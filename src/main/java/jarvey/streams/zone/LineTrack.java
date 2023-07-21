package jarvey.streams.zone;

import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.Point;

import com.google.gson.annotations.SerializedName;

import utils.geo.util.GeoUtils;

import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackEvent;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class LineTrack implements Timestamped {
	public static final String STATE_TRACKED = "Tracked";
	public static final String STATE_DELETED = "Deleted";
	public static final String[] STATES = new String[]{ STATE_TRACKED, STATE_DELETED };

	@SerializedName("node") private String m_nodeId;
	@SerializedName("track_id") private String m_trackId;
	@SerializedName("state") private String m_state;
	@SerializedName("line") private LineSegment m_line;
	@SerializedName("world_line") private LineSegment m_worldLine;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	public static LineTrack from(TrackEvent t0, TrackEvent t1) {
		if ( !t1.isDeleted() ) {
			Point p0 = GeoUtils.getCentroid(t0.getBox());
			Point p1 = GeoUtils.getCentroid(t1.getBox());
			
			return new LineTrack(t1.getNodeId(), t1.getTrackId(), STATE_TRACKED,
									GeoUtils.toLineSegment(p0, p1),
									GeoUtils.toLineSegment(t0.getWorldCoordinates(), t1.getWorldCoordinates()),
									t1.getFrameIndex(), t1.getTimestamp());
		}
		else {
			Point pt = GeoUtils.getCentroid(t0.getBox());
			return new LineTrack(t1.getNodeId(), t1.getTrackId(), STATE_DELETED,
									GeoUtils.toLineSegment(pt, pt),
									GeoUtils.toLineSegment(t0.getWorldCoordinates(), t0.getWorldCoordinates()),
									t1.getFrameIndex(), t1.getTimestamp());
		}
	}
	
	public LineTrack(String nodeId, String luid, String state, LineSegment line, LineSegment worldLine,
						long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_trackId = luid;
		m_state = state;
		m_line = line;
		m_worldLine = worldLine;
		m_frameIndex = frameIndex;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	public String getState() {
		return m_state;
	}
	
	public boolean isTracked() {
		return m_state.equals(STATE_TRACKED);
	}
	
	public boolean isDeleted() {
		return m_state.equals(STATE_DELETED);
	}
	
	public LineSegment getLine() {
		if ( isTracked() ) {
			return m_line;
		}
		else {
			throw new IllegalStateException("DELETED state");
		}
	}
	
	public LineSegment getWorldLine() {
		if ( isTracked() ) {
			return m_worldLine;
		}
		else {
			throw new IllegalStateException("DELETED state");
		}
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public String toString() {
		String lineStr = GeoUtils.toString(m_line, 1);
		return String.format("%s[node=%s, luid=%d, line=%s, frame_idx=%d, ts=%d]",
								m_state, m_nodeId, m_trackId, lineStr, m_frameIndex, m_ts);
	}
	
	public static String toString(Point pt) {
		return String.format("(%.3f,%.3f)", pt.getX(), pt.getY());
	}
}