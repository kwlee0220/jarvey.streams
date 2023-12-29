package jarvey.streams.optor;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;

import jarvey.streams.model.Timestamped;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DelayedTransformer<K,T extends Timestamped> implements Transformer<K, T, Iterable<KeyValue<K,T>>> {
	private ProcessorContext m_context;
	private final long m_delayMillis;
	private final MinMaxPriorityQueue<KeyValue<K,T>> m_queue;
	
	private Cancellable m_schedule;
	private boolean m_dirty;
	
	public DelayedTransformer(Duration delay) {
		m_delayMillis = delay.toMillis();
		m_queue = MinMaxPriorityQueue.orderedBy(COMPARATOR).create();
	}

    public void init(ProcessorContext context) {
		m_context = context;		
		m_schedule = m_context.schedule(Duration.ofMillis(m_delayMillis), PunctuationType.WALL_CLOCK_TIME,
										this::punctuate);
    }

	@Override
	public void close() {
		m_schedule.cancel();
	}

	@Override
	public Iterable<KeyValue<K,T>> transform(K key, T value) {
		m_queue.add(KeyValue.pair(key, value));

		return timeElapsed(value.getTimestamp());
	}
	
	private List<KeyValue<K,T>> timeElapsed(long ts) {
		List<KeyValue<K,T>> expireds = Lists.newArrayList();
		while ( m_queue.size() > 0  ) {
			long delay = ts - m_queue.peekFirst().value.getTimestamp();
			if ( delay <= m_delayMillis ) {
				break;
			}
			expireds.add(m_queue.removeFirst());
		}
		m_dirty = true;
		
		return expireds;
	}

	private void punctuate(long wallClock) {
		if ( !m_dirty && m_queue.size() > 0 ) {
			long ts = m_queue.peekLast().value.getTimestamp() + m_delayMillis;
			for ( KeyValue<K,T> kv: timeElapsed(ts) ) {
				m_context.forward(kv.key, kv.value);
			}
		}
	}
	
	private Comparator<KeyValue<K,T>> COMPARATOR = new Comparator<KeyValue<K,T>>() {
		@Override
		public int compare(KeyValue<K,T> kv1, KeyValue<K,T> kv2) {
			T v1 = kv1.value;
			T v2 = kv2.value;
			
			return Long.compare(v1.getTimestamp(), v2.getTimestamp());
		}
	};

}
