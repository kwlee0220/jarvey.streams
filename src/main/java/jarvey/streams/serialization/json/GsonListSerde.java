package jarvey.streams.serialization.json;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GsonListSerde<T> implements Serde<List<T>> {
	private final Class<T> m_cls;
	private final Gson m_gson;
	
	public GsonListSerde(Class<T> cls, Gson gson) {
		m_cls = cls;
		m_gson = gson;
	}

	@Override
	public JsonSerializer<T> serializer() {
		return new JsonSerializer<>(m_gson);
	}

	@Override
	public JsonDeserializer<T> deserializer() {
		return new JsonDeserializer<>(m_cls, m_gson);
	}
	
	public JsonElement toJsonTree(T obj) {
		return serializer().toJsonTree(obj);
	}
	
	public String toJson(List<T> obj) {
		return serializer().toJson(obj);
	}
	
	public T fromJson(String json) {
		return m_gson.fromJson(json, m_cls);
	}
	
	public static class JsonSerializer<T> implements Serializer<List<T>> {
		private final Gson m_gson;
		
		JsonSerializer(Gson gson) {
			m_gson = gson;
		}
		
		@Override
		public byte[] serialize(String topic, List<T> ev) {
			if ( ev != null ) {
				return toJson(ev).getBytes(StandardCharsets.UTF_8);
			}
			else {
				return null;
			}
		}
		
		public String toJson(List<T> ev) {
			return m_gson.toJson(ev);
		}
		
		public JsonElement toJsonTree(T ev) {
			return m_gson.toJsonTree(ev);
		}
	}
	
	static class JsonDeserializer<T> implements Deserializer<List<T>> {
		private final Gson m_gson;
		private final Type m_listType;
		
		JsonDeserializer(Class<T> cls, Gson gson) {
			m_gson = gson;
			m_listType = TypeToken.getParameterized(List.class, cls).getType();
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public List<T> deserialize(String topic, byte[] bytes) {
			if ( bytes != null ) {
				String json = new String(bytes, StandardCharsets.UTF_8);
				try {
					return (List<T>)m_gson.fromJson(json, m_listType);
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
