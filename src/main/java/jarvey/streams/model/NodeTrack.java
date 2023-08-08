package jarvey.streams.model;

import org.apache.kafka.common.serialization.Serde;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import jarvey.streams.serialization.json.GsonSerde;
import jarvey.streams.serialization.json.GsonUtils;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class NodeTrack implements ObjectTrack, Comparable<NodeTrack> {
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
	
	private final static GsonSerde<NodeTrack> SERDE = GsonUtils.getSerde(NodeTrack.class);
	public static final Serde<NodeTrack> getGsonSerde() {
		return SERDE;
	}
	
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
	public String getId() {
		return getTrackletId().toString();
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
		String bboxStr = (m_box != null) ? GeoUtils.toString(m_box, 0) : "null";
		String worldCoordStr = m_worldCoords != null ? GeoUtils.toString(m_worldCoords, 1) : "null";
		return String.format("%s[node=%s, track_id=%s, box=%s, world=%s, zone=%s, frame_idx=%d, ts=%d]",
								trackStr, m_nodeId, m_trackId,
								bboxStr, worldCoordStr,
								m_zoneRelation, m_frameIndex, m_ts);
	}

	public static Builder builder(String nodeId, String trackId, State state, long ts) {
		return new Builder(nodeId, trackId, state, ts);
	}
	public static class Builder {
		private final String m_nodeId;
		private final String m_trackId;
		private final State m_state;
		private final long m_ts;
		
		private Envelope m_box = null;
		private Point m_worldCoords = null;
		private double m_distance = -1;
		private long m_frameIndex = -1;
		private String m_zoneRelation = null;
		
		private Builder(String nodeId, String trackId, State state, long ts) {
			m_nodeId = nodeId;
			m_trackId = trackId;
			m_state = state;
			m_ts = ts;
		}
		
		public NodeTrack build() {
			NodeTrack track = new NodeTrack();
			track.m_nodeId = m_nodeId;
			track.m_trackId = m_trackId;
			track.m_state = m_state;
			track.m_box = m_box;
			track.m_frameIndex = m_frameIndex;
			track.m_ts = m_ts;
			track.m_worldCoords = m_worldCoords;
			track.m_zoneRelation = m_zoneRelation;
			
			return track;
		}
		
		public Builder setEnvelope(Envelope bbox) {
			m_box = bbox;
			return this;
		}
		
		public Builder setWorldCoordinate(Point coord) {
			m_worldCoords = coord;
			return this;
		}
		
		public Builder setDistance(double distance) {
			m_distance = distance;
			return this;
		}
		
		public Builder setZoneRelation(String zoneRelation) {
			m_zoneRelation = zoneRelation;
			return this;
		}
		
		public Builder setFrameIndex(int frameIndex) {
			m_frameIndex = frameIndex;
			return this;
		}
	}
}