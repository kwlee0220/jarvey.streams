package jarvey.streams.serialization.json;

import com.google.gson.annotations.SerializedName;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GsonKeyValue<K,V> {
	@SerializedName("key") private K m_key;
	@SerializedName("value") private V m_value;
	
	public static <K,V> GsonKeyValue<K,V> of(K key, V value) {
		return new GsonKeyValue<>(key, value);
	}
	
	private GsonKeyValue(K key, V value) {
		m_key = key;
		m_value = value;
	}
	
	public K getKey() {
		return m_key;
	}
	
	public V getValue() {
		return m_value;
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s", m_key, m_value);
	}
}