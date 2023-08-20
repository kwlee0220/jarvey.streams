package jarvey.streams.processor;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.LoggerSettable;
import utils.func.CheckedRunnable;
import utils.func.Funcs;
import utils.func.Unchecked;

import jarvey.streams.KafkaParameters;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractKafkaTopicProcessorDriver<K,V>
												implements LoggerSettable, CheckedRunnable, AutoCloseable {
	private static final Logger s_logger = LoggerFactory.getLogger(AbstractKafkaTopicProcessorDriver.class);
	
	private KafkaConsumer<K,V> m_consumer;
	@Mixin protected KafkaParameters m_kafkaParams;
	private boolean m_stopOnPollTimeout = false;
	private boolean m_updateOffset = true;
	
	private final Map<TopicPartition,KafkaTopicPartitionProcessor<K,V>> m_processors = Maps.newHashMap();
	private final Map<TopicPartition,OffsetAndMetadata> m_offsets = Maps.newHashMap();
	private Logger m_logger = s_logger;
	
	/**
	 * 본 topic processor가 읽을 topic 이름들을 반환한다.
	 * 
	 * @return	topic 이를 리스트.
	 */
	abstract protected Collection<String> getInputTopics();
	
	/**
	 * 주어진 topic partition을 처리할 {@link KafkaTopicPartitionProcessor} 객체를 할당하여 반환한다.
	 * 
	 * 생성된 processor는 전체 프로그램이 종료될 {@link KafkaTopicPartitionProcessor#close}를
	 * 통해 release된다.
	 * 생성된 processor는 다른 partition을 위해서도 재활용될 수 있다.
	 * 
	 * @param tpart	topic partition 정보.
	 * @return	{@link KafkaTopicPartitionProcessor} 객체.
	 * @throws Exception	processor 객체 생성 중 오류가 발생된 경우.
	 */
	abstract protected KafkaTopicPartitionProcessor<K,V> allocateProcessor(TopicPartition tpart) throws Exception;

	@Override
	public void close() throws Exception { }
	
	public boolean stopOnPollTimeout() {
		return m_stopOnPollTimeout;
	}
	@Option(names={"--stop-on-poll-timeout"})
	public void stopOnPollTimeout(boolean flag) {
		m_stopOnPollTimeout = flag;
	}

	@Option(names={"--no-update-offset"})
	public void noUpdateOffset(boolean flag) {
		m_updateOffset = !flag;
	}

	protected KafkaConsumer<K, V> openKafkaConsumer() {
		return new KafkaConsumer<>(m_kafkaParams.toConsumerProperties());
	}

	@Override
	public void run() throws Throwable {
		try ( KafkaConsumer<K,V> consumer = openKafkaConsumer() ) {
			m_consumer = consumer;
			
			Set<String> topics = Sets.newHashSet(getInputTopics());
			consumer.subscribe(topics, m_rebalanceListener);
			
			try {
				Map<TopicPartition,OffsetAndMetadata> offsets = Maps.newHashMap();
				long currentTs = -1;
				long lastEventTs = -1;
				
				// 첫번째 poll할 때는 'initial poll timeout'을 사용한다.
				Duration pollTimeout = m_kafkaParams.getInitialPollTimeout();
				
				while ( true ) {
					ConsumerRecords<K,V> records = consumer.poll(pollTimeout);
					
					// 두번재 이후 poll부터 'poll timeout'를 사용하도록 설정한다.
					pollTimeout = m_kafkaParams.getPollTimeout();
					
					if ( records.count() > 0 ) {
						if ( s_logger.isDebugEnabled() ) {
							s_logger.debug("fetch {} records", records.count());
						}
						
						for ( TopicPartition tpart: records.partitions() ) {
							List<ConsumerRecord<K,V>> recList = records.records(tpart);
							KafkaTopicPartitionProcessor<K,V> processor = getOrAllocateProcessor(tpart);
							processor.process(tpart, recList)
									.ifPresent(os -> m_offsets.put(tpart, os));
							
							// Kafka에서 읽은 마지막 consumer record에서 timestamp를 읽어서
							// current timestamp 값을 설정한다.
							long eventTs = processor.extractTimestamp(Funcs.getLast(recList));
							if ( currentTs == lastEventTs ) {
								// 이전에 추정치를 사용하지 않은 경우.
								currentTs = Math.max(currentTs, eventTs);
							}
							else {
								currentTs = eventTs;
							}
							lastEventTs = eventTs;
						}
					}
					else {
						// poll-timeout이 발생한 경우.
						
						// current timestamp의 예상치를 구하여 각 processor들에 대해 timeElapsed을 알림.
						// wall-clock으로 event time을 추정하는 것이기 때문에, 안전하게
						// 측정된 wall-clock 경과시간의 70% 정도만 경과한 것으로 추정치를 구한다.
						currentTs += Math.round(pollTimeout.toMillis() * 0.7);
						for ( KafkaTopicPartitionProcessor<K,V> proc: m_processors.values() ) {
							proc.timeElapsed(currentTs);
						}
						
						if ( m_stopOnPollTimeout ) {
							return;
						}
					}
					
					if ( m_updateOffset ) {
						consumer.commitSync(offsets);
					}
				}
			}
			finally {
				m_processors.forEach((k,proc) -> Unchecked.runOrIgnore(proc::close));
				m_processors.clear();
				
				Unchecked.runOrRTE(this::close);
			}
		}
		catch ( WakeupException ignored ) { }
		catch ( Exception e ) {
			s_logger.error("Unexpected error", e);
		}
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	private KafkaTopicPartitionProcessor<K,V> getOrAllocateProcessor(TopicPartition tpart) throws Exception {
		KafkaTopicPartitionProcessor<K,V> proc = m_processors.get(tpart);
		if ( proc == null ) {
			proc = allocateProcessor(tpart);
			m_processors.put(tpart, proc);
		}
		
		return proc;
	}
	
	private final ConsumerRebalanceListener m_rebalanceListener = new ConsumerRebalanceListener() {
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			s_logger.info("provoking topic-partitions: {}", partitions);

			Map<TopicPartition,OffsetAndMetadata> offsets = Maps.newHashMap();
			for ( TopicPartition tpart: partitions ) {
				KafkaTopicPartitionProcessor<K,V> processor = m_processors.remove(tpart);
				if ( processor != null ) {
					try {
						processor.close();
					}
					catch ( Exception e ) {
						s_logger.error("fails to close KafkaTopicProcessor {}, cause={}", processor, e);
					}
				}
				OffsetAndMetadata offset = m_offsets.remove(tpart);
				if ( offset != null ) {
					offsets.put(tpart, offset);
				}
			}
			
			if ( m_updateOffset ) {
				m_consumer.commitSync(offsets);
			}
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			s_logger.info("assigning new topic-partitions: {}", partitions);
		}
	};
}
