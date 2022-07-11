package jarvey.streams.model.old;

import java.io.IOException;
import java.util.Set;

import org.apache.kafka.common.serialization.Serde;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import jarvey.streams.serialization.json.EnvelopeAdater;
import jarvey.streams.serialization.json.JsonKafkaSerde;
import jarvey.streams.serialization.json.PointAdapter;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZoneChangedDAO {
	public static enum State {
		CHANGED(0),
		DELETED(1);
		
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
	@SerializedName("zones") private Set<String> m_zoneIds;
	@SerializedName("entered_zones") private Set<String> m_enteredZoneIds;
	@SerializedName("left_zones") private Set<String> m_leftZoneIds;
	@SerializedName("state") private State m_state;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	private static final Gson s_gson;
	private static final Serde<ZoneChangedDAO> s_serde;
	static {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(Point.class, new PointAdapter());
		builder.registerTypeAdapter(Envelope.class, new EnvelopeAdater());
		builder.registerTypeAdapter(State.class, new StateAdapter());
		s_gson = builder.create();
		
		s_serde = new JsonKafkaSerde<>(ZoneChangedDAO.class, s_gson);
	}
	
	public ZoneChangedDAO(String nodeId, long luid, Set<String> zoneIds, Set<String> enteredZoneIds,
							Set<String> leftZoneIds, State state, long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_luid = luid;
		m_zoneIds = zoneIds;
		m_enteredZoneIds = enteredZoneIds;
		m_leftZoneIds = leftZoneIds;
		m_state = state;
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
		return m_zoneIds;
	}
	
	public Set<String> getEnteredZones() {
		return m_enteredZoneIds;
	}
	
	public Set<String> getLeftZones() {
		return m_leftZoneIds;
	}
	
	public State getState() {
		return m_state;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public static ZoneChangedDAO fromJson(String gsonStr) {
		return s_gson.fromJson(gsonStr, ZoneChangedDAO.class);
	}
	
	@Override
	public String toString() {
		String tag = m_state.equals(State.CHANGED) ? "Changed" : "Deleted";
		return String.format("%s[node=%s, luid=%d, zones=%s, entereds=%s, lefts=%s, frame_idx=%d, ts=%d]",
								tag, m_nodeId, m_luid, m_zoneIds, m_enteredZoneIds, m_leftZoneIds,
								m_frameIndex, m_ts);
	}
	
	public static Serde<ZoneChangedDAO> getSerde() {
		return s_serde;
	}
	
	static class StateAdapter extends TypeAdapter<State> {
		@Override
		public State read(JsonReader in) throws IOException {
			String str = in.nextString();
			return State.valueOf(str);
		}

		@Override
		public void write(JsonWriter out, State pt) throws IOException {
			out.value(pt.name());
		}
	}
}