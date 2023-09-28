package jarvey.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface KafkaConsumerRecordProcessor<K,V> extends AutoCloseable {
	public ProcessResult process(ConsumerRecord<K,V> record);
	public long extractTimestamp(ConsumerRecord<K,V> record);
	public void timeElapsed(long expectedTs);
}
