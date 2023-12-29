package jarvey.streams.processor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Maps;

import utils.stream.FStream;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface KafkaTopicPartitionProcessor<K,V> extends AutoCloseable {
	public static class TopicOffset {
		private final TopicPartition m_partition;
		private final long m_offset;

		TopicOffset(TopicPartition partition, long offset) {
			m_partition = partition;
			m_offset = offset;
		}
		
		public TopicPartition partition() {
			return m_partition;
		}
		
		public long offset() {
			return m_offset;
		}
		
		public OffsetAndMetadata getOffsetAndMetadata() {
			return (m_offset > 0) ? new OffsetAndMetadata(m_offset) : null;
		}
		
		@Override
		public String toString() {
			return String.format("%d(%d)", m_partition, m_offset);
		}
	};
	
	public static class ProcessResult {
		private final Map<TopicPartition,OffsetAndMetadata> m_topicOffsets;
		private final boolean m_stop;
		
		public static final ProcessResult NULL = new ProcessResult(Collections.emptyMap(), false);
		
		public static ProcessResult empty() {
			return new ProcessResult(Maps.newHashMap(), false);
		}
		
		public static ProcessResult of(TopicPartition tpart, long offset) {
			return of(tpart, offset, false);
		}
		
		public static ProcessResult of(TopicPartition tpart, long offset, boolean stop) {
			Map<TopicPartition,OffsetAndMetadata> topicOffsets = Maps.newHashMap();
			topicOffsets.put(tpart, new OffsetAndMetadata(offset));
			
			return new ProcessResult(topicOffsets, stop);
		}
		
		public ProcessResult(Map<TopicPartition,OffsetAndMetadata> offsets, boolean stop) {
			m_topicOffsets = offsets;
			m_stop = false;
		}
		
		public void add(TopicPartition tpart, long offset) {
			OffsetAndMetadata prev = m_topicOffsets.get(tpart);
			if ( prev == null ) {
				m_topicOffsets.put(tpart, new OffsetAndMetadata(offset));
			}
			else if ( offset > prev.offset() ) {
				m_topicOffsets.put(tpart, new OffsetAndMetadata(offset));
			}
		}
		
		public ProcessResult combine(ProcessResult result) {
			Map<TopicPartition,OffsetAndMetadata> topicOffsets = 
				FStream.from(m_topicOffsets)
							.concatWith(FStream.from(result.m_topicOffsets))
							.groupByKey(kv -> kv.key(), kv -> kv.value())
							.stream()
							.mapValue(lst -> FStream.from(lst).max(to -> to.offset()).get())
							.toMap();
			return new ProcessResult(topicOffsets, m_stop || result.m_stop);
		}
		
		public Map<TopicPartition,OffsetAndMetadata> getTopicOffsetAndMetadataMap() {
			return m_topicOffsets;
		}
		
		public boolean stopProcess() {
			return m_stop;
		}
	}
	
	public ProcessResult process(TopicPartition tpart, List<ConsumerRecord<K,V>> partition)
		throws Exception;
	public default ProcessResult timeElapsed(TopicPartition tpart, long expectedTs) {
		return ProcessResult.NULL;
	}
	public long extractTimestamp(ConsumerRecord<K,V> record);
	
	public static <K,V> KafkaTopicPartitionProcessor<K,V> wrap(KafkaConsumerRecordProcessor<K, V> recordProc) {
		return new RecordLevelPartitionProcessor<>(recordProc);
	}
}
