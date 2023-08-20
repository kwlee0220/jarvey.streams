package jarvey.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import utils.func.FOption;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KeyValueStoreFStream<K,V> implements FStream<KeyValue<K,V>> {
	private final KeyValueIterator<K, V> m_kvIter;
	private KeyValue<K,V> m_current;
	
	public static <K, V> KeyValueStoreFStream<K,V> from(KeyValueStore<K, V> kvStore) {
		return new KeyValueStoreFStream<>(kvStore.all());
	}
	
	private KeyValueStoreFStream(KeyValueIterator<K, V> kvIter) {
		m_kvIter = kvIter;
		m_current = m_kvIter.hasNext() ? m_kvIter.next() : null;
	}

	@Override
	public void close() throws Exception {
		m_kvIter.close();
	}

	@Override
	public FOption<KeyValue<K, V>> next() {
		if ( m_current != null ) {
			KeyValue<K,V> kv = m_current;
			m_current = m_kvIter.next();
			return FOption.of(kv);
		}
		else {
			return FOption.empty();
		}
	}
}
