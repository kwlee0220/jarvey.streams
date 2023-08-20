package jarvey.streams;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.func.FOption;
import utils.func.Funcs;
import utils.func.KeyValue;
import utils.stream.FStream;
import utils.stream.Generator;

import jarvey.streams.model.Range;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class ConsumerRecordStream implements FStream<ConsumerRecord<String,byte[]>> {
	private final KafkaConsumer<String,byte[]> m_consumer;
	
	private final String m_topic;
	private final Duration m_initialPollTimeout;
	private final Duration m_pollTimeout;
	private final boolean m_stopOnPollTimeout;
	private final TopicPartition m_tpart;
	private final Range<Long> m_offsetRange;
	private final Set<String> m_keys;
	private final ConsumerRecordGenerator m_generator;
	
	private ConsumerRecordStream(Builder builder) {
		m_topic = builder.m_topic;
		m_initialPollTimeout = builder.m_initialPollTimeout;
		m_pollTimeout = builder.m_pollTimeout;
		m_stopOnPollTimeout = builder.m_stopOnPollTimeout;
		m_keys = builder.m_keys;

		m_consumer = new KafkaConsumer<>(builder.m_consumerProps);
		if ( builder.m_ranges.size() > 0 ) {
			Set<TopicPartition> tparts = FStream.from(builder.m_ranges)
												.map(KeyValue::key)
												.toSet();
			if ( tparts.size() > 1 ) {
				throw new IllegalArgumentException(
									String.format("Multiple partitions are not supported: %s", tparts));
			}
			m_tpart = Funcs.getFirst(tparts);
			m_consumer.assign(tparts);
			
			m_offsetRange = FStream.from(builder.m_ranges)
									.map(KeyValue::value)
									.reduce((r1, r2) -> r1.span(r2));
			m_consumer.seek(m_tpart, m_offsetRange.min());
		}
		else if ( m_topic != null ) {
			m_tpart = null;
			m_offsetRange = Range.all();
			m_consumer.subscribe(Arrays.asList(m_topic));
		}
		else {
			throw new IllegalArgumentException("topic or topic-partition is not specified");
		}
		
		m_generator = new ConsumerRecordGenerator(16);
	}

	@Override
	public void close() throws Exception {
		m_consumer.close();
	}

	@Override
	public FOption<ConsumerRecord<String, byte[]>> next() {
		return m_generator.next();
	}
	
	private class ConsumerRecordGenerator extends Generator<ConsumerRecord<String, byte[]>> {
		public ConsumerRecordGenerator(int length) {
			super(length);
		}

		@Override
		public void run() throws Throwable {
			Duration pollTimeout = m_initialPollTimeout;
			
			while ( true ) {
				ConsumerRecords<String, byte[]> records = m_consumer.poll(pollTimeout);
				if ( records.count() > 0 ) {
					for ( TopicPartition tpart: records.partitions() ) {
						if ( m_tpart == null || m_tpart.equals(tpart) ) {
							for ( ConsumerRecord<String, byte[]> record: records.records(tpart) ) {
								if ( m_keys != null && !m_keys.contains(record.key()) ) {
									continue;
								}
								
								int cmp = m_offsetRange.compareTo(record.offset());
								if ( cmp == 0 ) {
									supply(record);
								}
								else if ( cmp > 0 ) {
									continue;
								}
								else {
									return;
								}
							}
						}
					}
				}
				else if ( m_stopOnPollTimeout ) {
					return;
				}
				pollTimeout = m_pollTimeout;
			}
		}
	};
	
	public static Builder from(Properties props) {
		return new Builder(props);
	}
	public static class Builder {
		private Properties m_consumerProps;
		private String m_topic;
		private Duration m_initialPollTimeout = Duration.ofSeconds(10);
		private Duration m_pollTimeout = Duration.ofSeconds(5);
		private boolean m_stopOnPollTimeout = false;
		private Map<TopicPartition,Range<Long>> m_ranges = Maps.newHashMap();
		private Set<String> m_keys;
		
		public ConsumerRecordStream build() {
			return new ConsumerRecordStream(this);
		}
		
		private Builder(Properties props) {
			m_consumerProps = props;
		}
		
		public Builder topic(String topic) {
			m_topic = topic;
			return this;
		}
		
		public Builder pollTimeout(Duration timeout) {
			m_pollTimeout = timeout;
			return this;
		}
		
		public Builder initialPollTimeout(Duration timeout) {
			m_initialPollTimeout = timeout;
			return this;
		}
		
		public Builder stopOnPollTimeout(boolean flag) {
			m_stopOnPollTimeout = flag;
			return this;
		}
		
		public Builder addRange(TopicPartition tpart, Range<Long> offsetRange) {
			Range<Long> range = m_ranges.get(tpart);
			if ( range == null ) {
				range = offsetRange;
			}
			else {
				range = range.span(offsetRange);
			}
			m_ranges.put(tpart, range);
			return this;
		}
		
		public Builder key(Iterable<String> keys) {
			m_keys = Sets.newHashSet(keys);
			return this;
		}
	}
}
