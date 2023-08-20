package jarvey.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import utils.func.FOption;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface KafkaConsumerRecordProcessor<K,V> extends AutoCloseable {
	public FOption<OffsetAndMetadata> process(ConsumerRecord<K,V> record);
	public long extractTimestamp(ConsumerRecord<K,V> record);
	public void timeElapsed(long expectedTs);
}
