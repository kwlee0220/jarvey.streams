package jarvey.streams.zone;

import static utils.Utilities.checkState;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.Point;

import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.ObjectTrack;
import jarvey.streams.model.Timestamped;
import utils.geo.util.GeoUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class LineTrack implements Timestamped {
	@SerializedName("id") private String m_id;
	@Nullable @SerializedName("start_point") private Point m_startPoint;
	@Nullable @SerializedName("stop_point") private Point m_stopPoint;
	@SerializedName("ts") private long m_ts;
	
	public static LineTrack from(ObjectTrack from, ObjectTrack to) {
		return new LineTrack(to.getId(), from.getLocation(), to.getLocation(), to.getTimestamp());
	}
	
	public static LineTrack from(ObjectTrack t) {
		return new LineTrack(t.getId(), null, t.getLocation(), t.getTimestamp());
	}
	
	public static LineTrack DELETED(ObjectTrack track) {
		return new LineTrack(track.getId(), null, null, track.getTimestamp());
	}
	
	private LineTrack(String id, Point start, Point stop, long ts) {
		m_id = id;
		m_startPoint = start;
		m_stopPoint = stop;
		m_ts = ts;
	}
	
	public String getId() {
		return m_id;
	}
	
	public boolean isLineTrack() {
		return m_startPoint != null && m_stopPoint != null;
	}
	
	public boolean isPointTrack() {
		return m_startPoint == null;
	}
	
	public boolean isDeleted() {
		return m_stopPoint == null;
	}
	
	public LineSegment getLine() {
		checkState(isLineTrack(), () -> String.format("not line track: %s", this));
		
		return GeoUtils.toLineSegment(m_startPoint, m_stopPoint);
	}
	
	public Point getPoint() {
		checkState(isPointTrack(), () -> String.format("not point track: %s", this));
		
		return m_stopPoint;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public String toString() {
		if ( isLineTrack() ) {
			String lineStr = GeoUtils.toString(getLine(), 1);
			return String.format("Line[id=%s, line=%s, ts=%d]", m_id, lineStr, m_ts);
		}
		else if ( isDeleted() ) {
			return String.format("Line[id=%s, DELETED, ts=%d]", m_id, m_ts);
		}
		else if ( isPointTrack() ) {
			return String.format("Line[id=%s, point=%s, ts=%d]", m_id, GeoUtils.toString(m_stopPoint), m_ts);
		}
		
		throw new AssertionError();
	}
	
	public static String toString(Point pt) {
		return String.format("(%.3f,%.3f)", pt.getX(), pt.getY());
	}
}