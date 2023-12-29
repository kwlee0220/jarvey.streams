package jarvey.streams.optor;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import jarvey.streams.model.Timestamped;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DelayEvents<K,V extends Timestamped> implements Processor<K, V, K, V> {
	private static final Logger s_logger = LoggerFactory.getLogger(DelayEvents.class); 
	
	private ProcessorContext<K,V> m_context;
	private final long m_delayMillis;
	private final List<Record<K,V>> m_queue = Lists.newArrayList();
	private Cancellable m_schedule;
	private long m_lastTs = -1;
	private boolean m_dirty = false;

	public DelayEvents(Duration delay) {
		m_delayMillis = delay.toMillis();
	}

	@Override
	public void init(ProcessorContext<K,V> context) {
		m_context = context;
		m_schedule = context.schedule(Duration.ofMillis(m_delayMillis),
										PunctuationType.WALL_CLOCK_TIME, this::flushDelayEvents);
	}

	@Override
	public void close() {
		m_schedule.cancel();
	}

	@Override
	public void process(Record<K,V> record) {
		m_queue.add(record);
		forwardDelayedEvents(record.value().getTimestamp());
		
		m_lastTs = record.value().getTimestamp();
		m_dirty = true;
	}

	private void forwardDelayedEvents(long current) {
		while ( m_queue.size() > 0 ) {
			Record<K,V> head = m_queue.get(0);
			if ( (current - head.value().getTimestamp()) < m_delayMillis ) {
				return;
			}
			
			m_context.forward(m_queue.remove(0));
		}
	}

	private void flushDelayEvents(long ts) {
		if ( !m_dirty && m_lastTs >= 0 ) {
			// Punctuation 인터벌동안 event가 도착하지 않은 경우.
			m_lastTs += Math.round(m_delayMillis * 0.9);
			forwardDelayedEvents(m_lastTs);
		}
		
		m_dirty = false;
	}
}
