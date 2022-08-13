package jarvey.streams.zone;

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
public final class GlobalZoneId {
	@SerializedName("node") private final String m_nodeId;
	@SerializedName("zone") private final String m_zoneId;
	
	public GlobalZoneId(String nodeId, String zoneId) {
		m_nodeId = nodeId;
		m_zoneId = zoneId;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public String getZoneId() {
		return m_zoneId;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		GlobalZoneId other = (GlobalZoneId)obj;
		return Objects.equals(m_nodeId, other.m_nodeId)
				&& Objects.equals(m_zoneId, other.m_zoneId);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_nodeId, m_zoneId);
	}
	
	@Override
	public String toString() {
		return String.format("%s/%s", m_nodeId, m_zoneId);
	}
	
	public static GlobalZoneId fromString(String str) {
		int idx = str.lastIndexOf('/');
		String nodeId = str.substring(0, idx);
		String zoneId = str.substring(idx+1);
		
		return new GlobalZoneId(nodeId, zoneId);
	}
	
	public static final Serde<GlobalZoneId> getSerde() {
		return s_serde;
	}
	
	private final static Serde<GlobalZoneId> s_serde = new Serde<GlobalZoneId>() {
		@Override
		public Serializer<GlobalZoneId> serializer() {
			return s_serializer;
		}

		@Override
		public Deserializer<GlobalZoneId> deserializer() {
			return s_deserializer;
		}
	};
	
	private static final Serializer<GlobalZoneId> s_serializer = new Serializer<GlobalZoneId>() {
		@Override
		public byte[] serialize(String topic, GlobalZoneId gzone) {
			if ( gzone != null ) {
				return gzone.toString().getBytes(StandardCharsets.UTF_8);
			}
			else {
				return null;
			}
		}
	};
	
	private static final Deserializer<GlobalZoneId> s_deserializer = new Deserializer<GlobalZoneId>() {
		@Override
		public GlobalZoneId deserialize(String topic, byte[] bytes) {
			if ( bytes != null ) {
				String str = new String(bytes, StandardCharsets.UTF_8);
				return GlobalZoneId.fromString(str);
			}
			else {
				return null;
			}
		}
	};
}