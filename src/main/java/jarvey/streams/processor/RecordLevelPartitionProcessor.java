package jarvey.streams.processor;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;


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
	public ProcessResult process(TopicPartition tpart, List<ConsumerRecord<K, V>> partition) {
		ProcessResult accum = ProcessResult.NULL;
		for ( ConsumerRecord<K, V> rec: partition ) {
			ProcessResult result = m_recordProcessor.process(tpart, rec);
			
			// ProcessResult를 갱신한다.
			accum = accum.combine(result);
			if ( accum.stopProcess() ) {
				break;
			}
		}
		
		return accum;
	}

	@Override
	public ProcessResult timeElapsed(TopicPartition tpart, long expectedTs) {
		return m_recordProcessor.timeElapsed(tpart, expectedTs);
	}

	@Override
	public long extractTimestamp(ConsumerRecord<K, V> record) {
		return m_recordProcessor.extractTimestamp(record);
	}
}
