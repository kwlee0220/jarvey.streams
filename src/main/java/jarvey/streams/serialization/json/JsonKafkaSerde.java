package jarvey.streams.serialization.json;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JsonKafkaSerde<T> implements Serde<T> {
	private final Class<T> m_cls;
	private final Gson m_gson;
	
	public JsonKafkaSerde(Class<T> cls, Gson gson) {
		m_cls = cls;
		m_gson = gson;
	}

	@Override
	public Serializer<T> serializer() {
		return new JsonSerializer<>(m_gson);
	}

	@Override
	public Deserializer<T> deserializer() {
		return new JsonDeserializer<>(m_cls, m_gson);
	}
	
	static class JsonSerializer<T> implements Serializer<T> {
		private final Gson m_gson;
		
		JsonSerializer(Gson gson) {
			m_gson = gson;
		}
		
		@Override
		public byte[] serialize(String topic, T ev) {
			if ( ev != null ) {
				return m_gson.toJson(ev).getBytes(StandardCharsets.UTF_8);
			}
			else {
				return null;
			}
		}
	}
	
	static class JsonDeserializer<T> implements Deserializer<T> {
		private final Class<T> m_cls;
		private final Gson m_gson;
		
		JsonDeserializer(Class<T> cls, Gson gson) {
			m_cls = cls;
			m_gson = gson;
		}
		
		@Override
		public T deserialize(String topic, byte[] bytes) {
			if ( bytes != null ) {
				String json = new String(bytes, StandardCharsets.UTF_8);
				try {
					return m_gson.fromJson(json, m_cls);
				}
				catch ( JsonSyntaxException e ) {
					e.printStackTrace();
				}
				return null;
			}
			else {
				return null;
			}
		}
		
	}
}
