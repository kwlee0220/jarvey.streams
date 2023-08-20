package jarvey.streams.processor;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import utils.func.FOption;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface KafkaTopicPartitionProcessor<K,V> extends AutoCloseable {
	public FOption<OffsetAndMetadata> process(TopicPartition tpart, List<ConsumerRecord<K,V>> partition)
		throws Exception;
	public long extractTimestamp(ConsumerRecord<K,V> record);
	public default void timeElapsed(long expectedTs) { }
	
	public static <K,V> KafkaTopicPartitionProcessor<K,V> wrap(KafkaConsumerRecordProcessor<K, V> recordProc) {
		return new RecordLevelPartitionProcessor<>(recordProc);
	}
}
