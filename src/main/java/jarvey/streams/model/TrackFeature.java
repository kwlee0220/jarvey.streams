package jarvey.streams.model;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

import com.google.gson.annotations.SerializedName;

import utils.geo.util.GeoUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TrackFeature implements Timestamped {
	
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
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	@SerializedName("world_coord") private Point m_worldCoords;
	@SerializedName("distance") private double m_distance;
	@SerializedName("zone_relation") private String m_zoneRelation;
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	public TrackletId getTrackletId() {
		return new TrackletId(m_nodeId, m_trackId);
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
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public Point getWorldCoordinates() {
		return m_worldCoords;
	}
	
	public double getDistance() {
		return m_distance;
	}
	
	public String getZoneRelation() {
		return m_zoneRelation;
	}
	
	@Override
	public String toString() {
		String trackStr = (isDeleted()) ? "Deleted" : "Tracked";
		return String.format("%s[node=%s, track_id=%d, box=%s, world=%s, zone=%s, frame_idx=%d, ts=%d]",
								trackStr, m_nodeId, m_trackId,
								GeoUtils.toString(m_box, 0), GeoUtils.toString(m_worldCoords, 1),
								m_zoneRelation, m_frameIndex, m_ts);
	}
}