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
public final class GUID {
	@SerializedName("node") private final String m_node;
	@SerializedName("luid") private final long m_luid;
	
	public GUID(String node, long luid) {
		m_node = node;
		m_luid = luid;
	}
	
	public String getNodeId() {
		return m_node;
	}
	
	public long getLuid() {
		return m_luid;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		GUID other = (GUID)obj;
		return Objects.equals(m_node, other.m_node)
				&& Objects.equals(m_luid, other.m_luid);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_node, m_luid);
	}
	
	@Override
	public String toString() {
		return String.format("%s/%d", m_node, m_luid);
	}
	
	public static GUID fromString(String str) {
		int idx = str.lastIndexOf('/');
		String nodeId = str.substring(0, idx);
		long luid = Long.parseLong(str.substring(idx+1));
		
		return new GUID(nodeId, luid);
	}
	
	public static final Serde<GUID> getSerde() {
		return s_serde;
	}
	
	private final static Serde<GUID> s_serde = new Serde<GUID>() {
		@Override
		public Serializer<GUID> serializer() {
			return s_serializer;
		}

		@Override
		public Deserializer<GUID> deserializer() {
			return s_deserializer;
		}
	};
	
	private static final Serializer<GUID> s_serializer = new Serializer<GUID>() {
		@Override
		public byte[] serialize(String topic, GUID guid) {
			if ( guid != null ) {
				return guid.toString().getBytes(StandardCharsets.UTF_8);
			}
			else {
				return null;
			}
		}
	};
	
	private static final Deserializer<GUID> s_deserializer = new Deserializer<GUID>() {
		@Override
		public GUID deserialize(String topic, byte[] bytes) {
			if ( bytes != null ) {
				String str = new String(bytes, StandardCharsets.UTF_8);
				return GUID.fromString(str);
			}
			else {
				return null;
			}
		}
	};
}