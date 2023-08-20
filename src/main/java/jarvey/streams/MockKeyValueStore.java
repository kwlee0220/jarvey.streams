package jarvey.streams;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MockKeyValueStore<K,V> implements KeyValueStore<K,V> {
	private final String m_name;
	private final Map<Bytes,byte[]> m_store = Maps.newHashMap();
	private final Serde<K> m_keySerde;
	private final Serde<V> m_valueSerde;
	private boolean m_closed = false;
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	public MockKeyValueStore(String name, Serde<K> keySerde, Serde<V> valueSerde) {
		m_name = name;
		m_keySerde = keySerde;
		m_valueSerde = valueSerde;
	}
	
	public Logger getLogger() {
		return m_logger;
	}
	
	public void setLogger(Logger logger) {
		m_logger = logger;
	}

	@Override
	public String name() {
		return m_name;
	}

	@Override
	public void init(ProcessorContext context, StateStore root) {
		m_closed = false;
	}

	@Override
	public boolean persistent() {
		return false;
	}

	@Override
	public boolean isOpen() {
		return !m_closed;
	}

	@Override
	public void close() {
		m_closed = true;
	}

	@Override
	public void flush() {
	}

	@Override
	public V get(K key) {
		Bytes keyBytes = serializeKey(key);
		return deserializeValue(m_store.get(keyBytes));
	}

	@Override
	public KeyValueIterator<K, V> range(K from, K to) {
		throw new UnsupportedOperationException();
	}

	@Override
	public KeyValueIterator<K, V> all() {
		return new KeyValueIteratorImpl(m_store.entrySet().iterator());
	}

	@Override
	public long approximateNumEntries() {
		return m_store.size();
	}

	@Override
	public void put(K key, V value) {
		m_store.put(serializeKey(key), serializeValue(value));
	}

	@Override
	public V putIfAbsent(K key, V value) {
		Bytes keyBytes = serializeKey(key);
		byte[] valueBytes = serializeValue(value);
		
		byte[] prev = m_store.get(keyBytes);
		if ( prev == null ) {
			m_store.put(keyBytes, valueBytes);
			return null;
		}
		else {
			return deserializeValue(prev);
		}
	}

	@Override
	public void putAll(List<KeyValue<K, V>> entries) {
		entries.forEach(kv -> put(kv.key, kv.value));
	}

	@Override
	public V delete(K key) {
		byte[] valueBytes = m_store.remove(serializeKey(key));
		return (valueBytes != null) ? deserializeValue(valueBytes) : null;
	}
	
	@Override
	public String toString() {
		return m_store.toString();
	}
	
	private K deserializeKey(Bytes bytes) {
		return m_keySerde.deserializer().deserialize("", bytes.get());
	}
	private V deserializeValue(byte[] bytes) {
		return m_valueSerde.deserializer().deserialize("", bytes);
	}
	private Bytes serializeKey(K key) {
		return Bytes.wrap(m_keySerde.serializer().serialize("", key));
	}
	private byte[] serializeValue(V value) {
		return m_valueSerde.serializer().serialize("", value);
	}
	
	private final class KeyValueIteratorImpl implements KeyValueIterator<K,V> {
		private final PeekingIterator<Map.Entry<Bytes,byte[]>> m_iter;
		
		private KeyValueIteratorImpl(Iterator<Map.Entry<Bytes,byte[]>> iter) {
			m_iter = Iterators.peekingIterator(iter);
		}

		@Override
		public boolean hasNext() {
			return m_iter.hasNext();
		}

		@Override
		public KeyValue<K, V> next() {
			Entry<Bytes,byte[]> ent = m_iter.next();
			return KeyValue.pair(deserializeKey(ent.getKey()), deserializeValue(ent.getValue()));
		}

		@Override
		public void close() {
		}
		
		@Override
		public void remove() {
			m_iter.remove();
		}

		@Override
		public K peekNextKey() {
			Entry<Bytes,byte[]> ent = m_iter.peek();
			return deserializeKey(ent.getKey());
		}
	}
}
