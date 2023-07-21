package jarvey.streams.model;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.annotations.SerializedName;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TrackletId implements Comparable<TrackletId> {
	@SerializedName("node") private final String m_node;
	@SerializedName("track") private final String m_trackId;
	
	public TrackletId(String node, String trackId) {
		m_node = node;
		m_trackId = trackId;
	}
	
	public String getNodeId() {
		return m_node;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		TrackletId other = (TrackletId)obj;
		return Objects.equals(m_node, other.m_node)
				&& Objects.equals(m_trackId, other.m_trackId);
	}
	
	@Override
	public int compareTo(TrackletId o) {
		int cmp = m_node.compareTo(o.m_node);
		if ( cmp != 0 ) {
			return cmp;
		}
		
		return Long.compare(Long.parseLong(m_trackId), Long.parseLong(o.m_trackId));
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_node, m_trackId);
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", m_node, m_trackId);
	}
	
	public static TrackletId fromString(String str) {
		int idx = str.lastIndexOf('/');
		String nodeId = str.substring(0, idx);
		String trackId = str.substring(idx+1);
		
		return new TrackletId(nodeId, trackId);
	}
	
	public static final Serde<TrackletId> getSerde() {
		return s_serde;
	}
	
	private final static Serde<TrackletId> s_serde = new Serde<TrackletId>() {
		@Override
		public Serializer<TrackletId> serializer() {
			return s_serializer;
		}

		@Override
		public Deserializer<TrackletId> deserializer() {
			return s_deserializer;
		}
	};
	
	private static final Serializer<TrackletId> s_serializer = new Serializer<TrackletId>() {
		@Override
		public byte[] serialize(String topic, TrackletId guid) {
			if ( guid != null ) {
				return guid.toString().getBytes(StandardCharsets.UTF_8);
			}
			else {
				return null;
			}
		}
	};
	
	private static final Deserializer<TrackletId> s_deserializer = new Deserializer<TrackletId>() {
		@Override
		public TrackletId deserialize(String topic, byte[] bytes) {
			if ( bytes != null ) {
				String str = new String(bytes, StandardCharsets.UTF_8);
				return TrackletId.fromString(str);
			}
			else {
				return null;
			}
		}
	};
}