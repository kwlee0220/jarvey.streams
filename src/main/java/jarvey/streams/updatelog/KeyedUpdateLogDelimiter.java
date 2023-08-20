package jarvey.streams.updatelog;

import java.time.Duration;
import java.util.List;

import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.streams.MockKeyValueStore;
import jarvey.streams.model.Range;
import jarvey.streams.serialization.json.GsonUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KeyedUpdateLogDelimiter<T extends KeyedUpdate>
								implements ValueTransformerWithKey<String, T, Iterable<KeyedUpdateIndex>> {
	private static final Logger s_logger = LoggerFactory.getLogger(KeyedUpdateLogDelimiter.class);
	
	private ProcessorContext m_context;
	private final String m_storeName;
	private final Duration m_timeToLive;
	private final boolean m_useMockStore;
	
	private KeyValueStore<String, KeyedUpdateIndex> m_store;
	
	private Cancellable m_schedule;
	private Duration m_scheduleInterval = Duration.ofMinutes(1);
	
	private int m_hitCount = 0;
	
	public KeyedUpdateLogDelimiter(String storeName, Duration timeToLive, boolean useMockStore) {
		m_storeName = storeName;
		m_timeToLive = timeToLive;
		m_useMockStore = useMockStore;
		
		if ( m_useMockStore ) {
			m_store = new MockKeyValueStore<>(storeName, Serdes.String(),
												GsonUtils.getSerde(KeyedUpdateIndex.class));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		m_context = context;

		if ( !m_useMockStore ) {
			m_store = (KeyValueStore<String, KeyedUpdateIndex>)context.getStateStore(m_storeName);
		}
		m_schedule = context.schedule(m_scheduleInterval, PunctuationType.WALL_CLOCK_TIME,
										this::handleOldTracklet);
	}

	@Override
	public void close() {
		m_schedule.cancel();
	}

	@Override
	public Iterable<KeyedUpdateIndex> transform(String topicKey, T update) {
		List<KeyedUpdateIndex> output = Lists.newArrayList();
		
		String key = update.getKey();
		KeyedUpdateIndex index = m_store.get(key);
		if ( index == null ) {	// 주어진 key에 대한 첫번째 갱신인 경우.
			Range<Long> initOffsetRange = Range.atLeast(m_context.offset());
			Range<Long> initTsRange = Range.at(update.getTimestamp());
			index = new KeyedUpdateIndex(key, m_context.partition(), initOffsetRange, initTsRange, 1);
			m_store.put(key, index);
			
			output.add(index);
		}
		else {
			index.update(update.getTimestamp());
			m_store.put(key, index);
		}
		
		if ( update.isLastUpdate() ) {
			index.update(update.getTimestamp());
			index.setLastUpdate(m_context.offset());
			m_store.delete(key);
			
			output.add(index);
		}
		
		purgeOldTracklets(update.getTimestamp());
		m_hitCount = 0;
		
		return output;
	}
	
	public void handleOldTracklet(long ts) {
		Duration elapsed = Duration.ofMillis(m_scheduleInterval.toMillis() * m_hitCount);	
		if ( elapsed.compareTo(m_timeToLive) > 0 ) {
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("wall-clock elapsed: {} -> clear store (entries={})",
								elapsed, m_store.approximateNumEntries());
			}
			
			// KeyStore에 등록된 모든 index를 제거한다.
			try ( KeyValueIterator<String, KeyedUpdateIndex> iter = m_store.all() ) {
				while ( iter.hasNext() ) {
					iter.next();
					iter.remove();
				}
			}
		}
		else if ( !elapsed.isZero() && s_logger.isInfoEnabled() ) {
			s_logger.info("wall-clock elapsed: {}", elapsed);
		}
		
		++m_hitCount;
	}
	
	private List<String> purgeOldTracklets(long ts) {
		List<String> purgeds = Lists.newArrayList();
		try ( KeyValueIterator<String, KeyedUpdateIndex> iter = m_store.all() ) {
			while ( iter.hasNext() ) {
				KeyValue<String, KeyedUpdateIndex> kv = iter.next();
				
				Duration maxElapsed = Duration.ofMillis(ts - kv.value.getTimestampRange().min());
				if ( maxElapsed.compareTo(m_timeToLive) > 0 ) {
					purgeds.add(kv.key);
					iter.remove();
				}
			}
		}
		
		return purgeds;
	}
}
