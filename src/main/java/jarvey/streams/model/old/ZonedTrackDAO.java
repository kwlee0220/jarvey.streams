package jarvey.streams.model.old;

import java.io.IOException;
import java.util.Set;

import org.apache.kafka.common.serialization.Serde;
import org.locationtech.jts.geom.Point;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import jarvey.streams.model.ObjectTrack;
import jarvey.streams.serialization.json.JsonKafkaSerde;
import jarvey.streams.serialization.json.PointAdapter;
import utils.geo.util.GeoUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZonedTrackDAO {
	public static enum State {
		MATCHED(0),
		UNMATCHED(1),
		DELETED(2);
		
		final private int m_id;
		
		private State(int id) {
			m_id = id;
		}
		
		public int getId() {
			return m_id;
		}
	}

	@SerializedName("node") private String m_nodeId;
	@SerializedName("luid") private long m_luid;
	@SerializedName("zones") private Set<String> m_zones;
	@SerializedName("state") private State m_state;
	@SerializedName("location") private Point m_location;
	@SerializedName("world_location") private Point m_worldLocation;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	private static final Gson s_gson;
	private static final Serde<ZonedTrackDAO> s_serde;
	static {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(Point.class, new PointAdapter());
		builder.registerTypeAdapter(State.class, new StateAdapter());
		s_gson = builder.create();
		
		s_serde = new JsonKafkaSerde<>(ZonedTrackDAO.class, s_gson);
	}
	
	public static ZonedTrackDAO MATCHED(ObjectTrack track, Set<String> zones) {
		return new ZonedTrackDAO(track.getNodeId(), track.getLuid(), State.MATCHED, zones,
								GeoUtils.getCentroid(track.getBox()), track.getWorldCoordinates(),
								track.getFrameIndex(), track.getTimestamp());
	}
	
	public static ZonedTrackDAO UNMATCHED(ObjectTrack track) {
		return new ZonedTrackDAO(track.getNodeId(), track.getLuid(), State.UNMATCHED, null,
								GeoUtils.getCentroid(track.getBox()), track.getWorldCoordinates(),
								track.getFrameIndex(), track.getTimestamp());
	}
	
	public static ZonedTrackDAO DELETED(ObjectTrack track) {
		return new ZonedTrackDAO(track.getNodeId(), track.getLuid(), State.DELETED, null,
								GeoUtils.getCentroid(track.getBox()), track.getWorldCoordinates(),
								track.getFrameIndex(), track.getTimestamp());
	}
	
	private ZonedTrackDAO(String nodeId, long luid, State state, Set<String> zones, Point centroid,
						Point worldLocation, long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_luid = luid;
		m_zones = zones;
		m_state = state;
		m_location = centroid;
		m_worldLocation = worldLocation;
		m_frameIndex = frameIndex;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public long getLuid() {
		return m_luid;
	}
	
	public Set<String> getZones() {
		return m_zones;
	}
	
	public State getState() {
		return m_state;
	}
	
	public Point getLocation() {
		return m_location;
	}
	
	public Point getWorldLocation() {
		return m_worldLocation;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public static ZonedTrackDAO fromJson(String gsonStr) {
		return s_gson.fromJson(gsonStr, ZonedTrackDAO.class);
	}
	
	@Override
	public String toString() {
		return String.format("%s[node=%s, luid=%d, zones=%s, location=%s, world=%s, frame_idx=%d, ts=%d]",
								m_state, m_nodeId, m_luid, m_zones,
								GeoUtils.toString(m_location), GeoUtils.toString(m_worldLocation),
								m_frameIndex, m_ts);
	}
	
	public static Serde<ZonedTrackDAO> getSerde() {
		return s_serde;
	}
	
	static class StateAdapter extends TypeAdapter<State> {
		@Override
		public State read(JsonReader in) throws IOException {
			return State.valueOf(in.nextString());
		}

		@Override
		public void write(JsonWriter out, State pt) throws IOException {
			out.value(pt.name());
		}
	}
}