package jarvey.streams.processor;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import utils.func.FOption;
import utils.stream.FStream;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordLevelPartitionProcessor<K,V> implements KafkaTopicPartitionProcessor<K, V> {
	private final KafkaConsumerRecordProcessor<K, V> m_recordProcessor;
	
	public RecordLevelPartitionProcessor(KafkaConsumerRecordProcessor<K, V> recordProcessor) {
		m_recordProcessor = recordProcessor;
	}

	@Override
	public void close() throws Exception {
		m_recordProcessor.close();
	}

	@Override
	public FOption<OffsetAndMetadata> process(TopicPartition tpart, List<ConsumerRecord<K, V>> partition) {
		return FStream.from(partition)
						.flatMapOption(m_recordProcessor::process)
						.findLast();
	}

	@Override
	public long extractTimestamp(ConsumerRecord<K, V> record) {
		return m_recordProcessor.extractTimestamp(record);
	}

	@Override
	public void timeElapsed(long expectedTs) {
		m_recordProcessor.timeElapsed(expectedTs);
	}
}
