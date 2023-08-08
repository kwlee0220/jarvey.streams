package jarvey.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.stream.FStream;
import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UpdateTimeAssociatedKeyValueStore<K,V> implements KeyValueStore<K,V> {
	private final KeyValueStore<K,V> m_store;
	private final LastUpdated<K> m_lastUpdateds = new LastUpdated<>();
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	public static <K,V> UpdateTimeAssociatedKeyValueStore<K,V> of(KeyValueStore<K,V> store) {
		return new UpdateTimeAssociatedKeyValueStore<>(store);
	}
	
	private UpdateTimeAssociatedKeyValueStore(KeyValueStore<K,V> store) {
		m_store = store;
	}
	
	public Logger getLogger() {
		return m_logger;
	}
	
	public void setLogger(Logger logger) {
		m_logger = logger;
	}

	@Override
	public String name() {
		return m_store.name();
	}

	@SuppressWarnings("deprecation")
	@Override
	public void init(ProcessorContext context, StateStore root) {
		m_store.init(context, root);
	}

	@Override
	public boolean persistent() {
		return m_store.persistent();
	}

	@Override
	public boolean isOpen() {
		return m_store.isOpen();
	}

	@Override
	public void close() {
		m_store.close();
	}

	@Override
	public void flush() {
		m_store.flush();
	}

	@Override
	public V get(K key) {
		return m_store.get(key);
	}
	
	public Instant getLastUpdated(K key) {
		return m_lastUpdateds.get(key);
	}

	@Override
	public KeyValueIterator<K, V> range(K from, K to) {
		return m_store.range(from, to);
	}

	@Override
	public KeyValueIterator<K, V> all() {
		return m_store.all();
	}

	@Override
	public long approximateNumEntries() {
		return m_store.approximateNumEntries();
	}

	@Override
	public void put(K key, V value) {
		m_store.put(key, value);
		m_lastUpdateds.update(key);
		
//		checkValidity();
	}

	@Override
	public V putIfAbsent(K key, V value) {
		V old = m_store.putIfAbsent(key, value);
		if ( old != null ) {
			m_lastUpdateds.update(key);
		}
		
		return old;
	}

	@Override
	public void putAll(List<KeyValue<K, V>> entries) {
		putAll(entries);
		FStream.from(entries)
				.forEach(kv -> m_lastUpdateds.update(kv.key));
	}

	@Override
	public V delete(K key) {
//		checkValidity();
		
		m_lastUpdateds.remove(key);
		return m_store.delete(key);
	}
	
	public void deleteOldEntries(Duration threshold) {
		getLogger().debug("starting to delete old entries: store={}", m_store.name());
		
		for ( KeyValue<K,Duration> kv: getInactiveAll(threshold) ) {
			if ( getLogger().isInfoEnabled() ) {
				String elapsedStr = (kv.value != null) ? String.format("%dm", kv.value.toMinutes()) : "unknown";
				getLogger().info("delete unfinished track: zone={}, elapsed={}", kv.key, elapsedStr);
			}
			delete(kv.key);
		}
		
		getLogger().debug("finishing deleting old entries: store={}", m_store.name());
	}
	
	public KVFStream<K,Instant> getLastUpdatedAll() {
		return m_lastUpdateds.getAll();
	}
	
	private List<KeyValue<K,Duration>> getInactiveAll(Duration threshold) {
		List<KeyValue<K,Duration>> inactives = Lists.newArrayList();
		
		Instant now = Instant.now();
		try ( KeyValueIterator<K, V> iter = m_store.all() ) {
			while ( iter.hasNext() ) {
				KeyValue<K,V> kv = iter.next();
				
				Instant lu = m_lastUpdateds.get(kv.key);
				if ( lu != null ) {
					Duration age = Duration.between(lu, now);
//					System.out.printf("key=%s, age=%dm\n", kv.key, age.toMinutes());
					if ( age.compareTo(threshold) > 0 ) {
						inactives.add(KeyValue.pair(kv.key, age));
					}
				}
				else {
					inactives.add(KeyValue.pair(kv.key, null));
				}
			}
		}
		
		return inactives;
	}
	
	private void checkValidity() {
		Set<K> keySet1 = m_lastUpdateds.getAll().toKeyStream().toSet();
		Set<K> keySet2 = FStream.from(m_store.all()).map(kv -> kv.key).toSet();
		if ( !keySet1.equals(keySet2) ) {
			getLogger().warn("incompatible stores: name={}, state-store={}, last-updateds={}",
								name(), keySet1, keySet2);
//			throw new IllegalStateException(String.format("incompatible stores: name=%s, state-store=%s, last-updateds=%s",
//															name(), keySet1, keySet2));
		}
	}
}
