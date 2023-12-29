package jarvey.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface KafkaConsumerRecordProcessor<K,V> extends AutoCloseable {
	public ProcessResult process(TopicPartition tpart, ConsumerRecord<K,V> record);
	public ProcessResult timeElapsed(TopicPartition tpart, long expectedTs);
	public long extractTimestamp(ConsumerRecord<K,V> record);
}
