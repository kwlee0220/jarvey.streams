package jarvey.streams.processor;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface KafkaTopicPartitionProcessor<K,V> extends AutoCloseable {
	public static class ProcessResult {
		private final OffsetAndMetadata m_offset;
		private final boolean m_continue;
		
		public ProcessResult(OffsetAndMetadata offset, boolean cont) {
			m_offset = offset;
			m_continue = cont;
		}
		
		public ProcessResult(long offset, boolean cont) {
			m_offset = new OffsetAndMetadata(offset);
			m_continue = cont;
		}
		
		public ProcessResult(long offset) {
			this(offset, true);
		}
		
		public OffsetAndMetadata getOffsetAndMetadata() {
			return m_offset;
		}
		
		public boolean getContinue() {
			return m_continue;
		}
	}
	
	public ProcessResult process(TopicPartition tpart, List<ConsumerRecord<K,V>> partition)
		throws Exception;
	public long extractTimestamp(ConsumerRecord<K,V> record);
	public default void timeElapsed(long expectedTs) { }
	
	public static <K,V> KafkaTopicPartitionProcessor<K,V> wrap(KafkaConsumerRecordProcessor<K, V> recordProc) {
		return new RecordLevelPartitionProcessor<>(recordProc);
	}
}
