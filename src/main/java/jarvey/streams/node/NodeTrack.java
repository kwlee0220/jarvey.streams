package jarvey.streams.node;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import utils.geo.util.GeoUtils;
import utils.stream.FStream;

import jarvey.streams.model.ObjectTrack;
import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;
import jarvey.streams.model.ZoneRelation;
import jarvey.streams.updatelog.KeyedUpdate;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class NodeTrack implements Timestamped, ObjectTrack, KeyedUpdate, Comparable<NodeTrack> {
	public static enum State {
		Tentative("T"),
		Confirmed("C"),
		TemporarilyLost("L"),
		Deleted("D");
		
		private final String m_abbr;
		
		State(String abbr) {
			m_abbr = abbr;
		}
		
		public String getAbbr() {
			return m_abbr;
		}
		
		public static State fromAbbr(String abbr) {
			return FStream.of(State.values())
							.findFirst(s -> s.getAbbr().equals(abbr))
							.getOrNull();
		}
	}

	@SerializedName("node") private String m_nodeId;
	@SerializedName("track_id") private String m_trackId;
	@SerializedName("state") private State m_state;
	@SerializedName("location") private Envelope m_box;
	@SerializedName("world_coord") private Point m_worldCoords;
	@SerializedName("distance") private double m_distance;
	@SerializedName("zone_expr") private ZoneRelation m_zoneRelation;
	@SerializedName("first_ts") private long m_firstTs;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	public TrackletId getTrackletId() {
		return new TrackletId(m_nodeId, m_trackId);
	}

	@Override
	public String getKey() {
		return getTrackletId().toString();
	}

	@Override
	public boolean isLastUpdate() {
		return isDeleted();
	}

	@Override
	public Point getLocation() {
		return getWorldCoordinates();
	}
	
	public State getState() {
		return m_state;
	}
	
	public String getStateAbbr() {
		return m_state.getAbbr();
	}
	
	public boolean isDeleted() {
		return m_state.equals(State.Deleted);
	}
	
	public Envelope getBox() {
		return m_box;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getAge() {
		return m_ts - m_firstTs;
	}
	
	public long getFirstTimestamp() {
		return m_firstTs;
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
	
	public ZoneRelation getZoneRelation() {
		return m_zoneRelation;
	}
	
	public boolean isSameTrack(NodeTrack other) {
		return Objects.equal(m_nodeId, other.m_nodeId) && Objects.equal(m_trackId, other.getTrackId());
	}
	
	@Override
	public int compareTo(NodeTrack o) {
		int cmp = Long.compare(m_ts, o.m_ts);
		if ( cmp != 0 ) {
			return cmp;
		}
		
		return getTrackletId().compareTo(o.getTrackletId());
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(m_ts, getTrackId());
	}
	
	@Override
	public String toString() {
		String trackStr = (isDeleted()) ? "Deleted" : "Tracked";
//		String bboxStr = (m_box != null) ? GeoUtils.toString(m_box, 0) : "null";
		String worldCoordStr = m_worldCoords != null ? GeoUtils.toString(m_worldCoords, 1) : "null";
		return String.format("%s[%s, world=%s, frame_idx=%d, ts=%d]",
								trackStr, getTrackletId(), worldCoordStr, m_frameIndex, m_ts);
	}
}