package jarvey.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import com.google.common.collect.Maps;

import utils.Holder;
import utils.func.FOption;
import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LastUpdated<K> {
	private final Map<K,Holder<Instant>> m_table = Maps.newHashMap();
	
	public void update(K key) {
		Holder<Instant> ts = m_table.get(key);
		if ( ts != null ) {
			ts.set(Instant.now());
		}
		else {
			m_table.put(key,  Holder.of(Instant.now()));
		}
	}
	
	public Instant get(K key) {
		return FOption.ofNullable(m_table.get(key)).map(Holder::get).getOrNull();
	}
	
	public Duration getElapsed(K key, Instant now) {
		Holder<Instant> ts = m_table.get(key);
		if ( ts != null ) {
			return Duration.between(ts.get(), now);
		}
		else {
			return null;
		}
	}
	
	public Instant remove(K key) {
		return FOption.ofNullable(m_table.remove(key)).map(Holder::get).getOrNull();
	}
	
	public KVFStream<K,Instant> getAll() {
		return KVFStream.from(m_table)
						.mapValue(Holder::get);
	}
}
