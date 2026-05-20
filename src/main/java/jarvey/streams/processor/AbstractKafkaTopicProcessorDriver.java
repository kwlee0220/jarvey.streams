package jarvey.streams.processor;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;

import com.google.common.collect.Maps;

import utils.LoggerNameBuilder;
import utils.LoggerSettable;
import utils.Throwables;
import utils.UnitUtils;
import utils.func.CheckedRunnable;
import utils.func.Funcs;
import utils.func.Unchecked;
import utils.stream.KeyValueFStream;

import jarvey.streams.KafkaOptions;
import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractKafkaTopicProcessorDriver<K,V>
												implements LoggerSettable, CheckedRunnable, AutoCloseable {
	private static final Logger s_logger = LoggerNameBuilder.from(AbstractKafkaTopicProcessorDriver.class)
																.dropSuffix(1)
																.append("driver")
																.getLogger();
	
	private static final String RANDOM_GROUP_ID = "random";
	private static final String KEY_DESERIALIZER_NAME = Serdes.String().deserializer().getClass().getName();
	private static final String VALUE_DESERIALIZER_NAME = Serdes.ByteArray().deserializer().getClass().getName();

	@Mixin protected KafkaOptions m_kafkaOptions;
	private KafkaConsumer<K,V> m_consumer;
	private boolean m_stopOnPollTimeout = false;
	
	private final Map<TopicPartition,KafkaTopicPartitionProcessor<K,V>> m_processors = Maps.newHashMap();
	private final Map<TopicPartition,OffsetAndMetadata> m_commitOffsets = Maps.newHashMap();
	private Logger m_logger = s_logger;

	private String m_groupId = getDefaultGroupId();
	public String getGroupId() {
		if ( m_groupId.equalsIgnoreCase(RANDOM_GROUP_ID) ) {
			m_groupId = UUID.randomUUID().toString();
		}
		return m_groupId;
	}
	@Option(names={"--group_id"}, paramLabel="id", description={"Kafka group id"})
	public void setGroupId(String grpId) {
		m_groupId = grpId;
	}

	private Duration m_pollTimeout = Duration.ofMillis(UnitUtils.parseDurationMillis("1s"));
	public Duration getPollTimeout() {
		return m_pollTimeout;
	}
	@Option(names={"--poll_timeout"}, paramLabel="duration", description="default: '1s'")
	public void setPollTimeout(String durStr) {
		m_pollTimeout = Duration.ofMillis(UnitUtils.parseDurationMillis(durStr));
	}

	private Duration m_timeout = Duration.ofSeconds(10);
	public Duration getTimeout() {
		return m_timeout;
	}
	@Option(names={"--initial_timeout"}, paramLabel="duration", description="default: '3s'")
	public void setTimeout(String durStr) {
		m_timeout = Duration.ofMillis(UnitUtils.parseDurationMillis(durStr));
	}
	
	public KafkaOptions getKafkaOptions() {
		return m_kafkaOptions;
	}
	
	/**
	 * 본 Kafka client에게 부여할 default group_id를 반환한다.
	 * 
	 * @return	group_id.
	 * 
	 * @return
	 */
	abstract protected String getDefaultGroupId();
	
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
	 * 생성된 processor는 다른 partition을 위해서도 공통으로 활용될 수 있다.
	 * 
	 * @param tpart	topic {@link TopicPartition} 정보.
	 * @return	{@link KafkaTopicPartitionProcessor} 객체.
	 * @throws Exception	processor 객체 생성 중 오류가 발생된 경우.
	 */
	abstract protected KafkaTopicPartitionProcessor<K,V> allocateProcessor(TopicPartition tpart) throws Exception;
	
	protected AbstractKafkaTopicProcessorDriver() {
		setLogger(s_logger);
	}

	@Override
	public void close() throws Exception {
		m_processors.forEach((tpart,proc) -> Unchecked.runOrIgnore(proc::close));
		m_processors.clear();
	}
	
	public boolean stopOnPollTimeout() {
		return m_stopOnPollTimeout;
	}
	@Option(names={"--stop_on_timeout"})
	public void stopOnPollTimeout(boolean flag) {
		m_stopOnPollTimeout = flag;
	}
	
	@Option(names={"--cleanup"}, description="clean-up consumer group id.")
	private boolean m_cleanUp = false;

	protected KafkaConsumer<K, V> openKafkaConsumer() {
		Properties props = m_kafkaOptions.toConsumerProperties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, getGroupId());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_NAME);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_NAME);
		return new KafkaConsumer<>(props);
	}

	@Override
	public void run() throws Exception {
		if ( m_cleanUp ) {
			cleanUp();
		}
		
		try ( KafkaConsumer<K,V> consumer = openKafkaConsumer() ) {
			m_consumer = consumer;
			
			consumer.subscribe(getInputTopics(), m_rebalanceListener);
			
			try {
				long currentTs = -1;
				long lastEventTs = -1;
				long lastPollTs = -1;
				
				while ( true ) {
					if ( lastPollTs < 0 ) {
						lastPollTs = System.currentTimeMillis();
					}
					ConsumerRecords<K,V> records = consumer.poll(getPollTimeout());
					
					if ( records.count() > 0 ) {
						if ( getLogger().isDebugEnabled() ) {
							getLogger().debug("fetch {} records", records.count());
						}
						
						for ( TopicPartition tpart: records.partitions() ) {
							List<ConsumerRecord<K,V>> recList = records.records(tpart);
							KafkaTopicPartitionProcessor<K,V> processor = getOrAllocateProcessor(tpart);
							ProcessResult result = processor.process(tpart, recList);
							updateCommitOffsets(result);
							
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
							
							// Processor가 추가의 데이터를 원하지 않는 경우는 종료시킨다.
							if ( result.stopProcess() ) {
								consumer.commitSync(m_commitOffsets);
								return;
							}
						}
						lastPollTs = -1;
					}
					else {
						// poll-timeout이 발생한 경우.
						long elapsedMillis = System.currentTimeMillis() - lastPollTs;
						if ( m_stopOnPollTimeout && elapsedMillis > getTimeout().toMillis() ) {
							return;
						}
						
						// current timestamp의 예상치를 구하여 각 processor들에 대해 timeElapsed을 알림.
						// wall-clock으로 event time을 추정하는 것이기 때문에, 안전하게
						// 측정된 wall-clock 경과시간의 95% 정도만 경과한 것으로 추정치를 구한다.
						long expectedTs = currentTs + elapsedMillis;
						KeyValueFStream.from(m_processors)
										.map((tp, proc) -> proc.timeElapsed(tp, expectedTs))
										.forEach(this::updateCommitOffsets);
					}

					consumer.commitSync(m_commitOffsets);
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
			getLogger().error("Unexpected error", e);
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
	
	private void updateCommitOffsets(ProcessResult result) {
		KeyValueFStream.from(result.getTopicOffsetAndMetadataMap())
						.forEach((tp,meta) -> {
							OffsetAndMetadata prev = m_commitOffsets.get(tp);
							if ( prev == null ) {
								m_commitOffsets.put(tp, meta);
							}
							else if ( meta.offset() > prev.offset() ) {
								m_commitOffsets.put(tp, meta);
							}
						});
	}
	
	private void cleanUp() throws InterruptedException, ExecutionException {
		try {
			m_kafkaOptions.deleteConsumerGroup(m_groupId);
		}
		catch ( InterruptedException e ) {
			throw e;
		}
		catch ( ExecutionException e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			if ( cause instanceof GroupIdNotFoundException ) { }
			else {
				getLogger().error("fails to delete consumer-group: id={}, cause={}", m_groupId, cause);
				throw e;
			}
		}
	}
	
	private final ConsumerRebalanceListener m_rebalanceListener = new ConsumerRebalanceListener() {
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			getLogger().info("provoking topic-partitions: {}", partitions);

			Map<TopicPartition,OffsetAndMetadata> offsets = Maps.newHashMap();
			for ( TopicPartition tpart: partitions ) {
				KafkaTopicPartitionProcessor<K,V> processor = m_processors.remove(tpart);
				if ( processor != null ) {
					try {
						processor.close();
					}
					catch ( Exception e ) {
						getLogger().error("fails to close KafkaTopicProcessor {}, cause={}", processor, e);
					}
				}
				OffsetAndMetadata offset = m_commitOffsets.remove(tpart);
				if ( offset != null ) {
					offsets.put(tpart, offset);
				}
			}

			m_consumer.commitSync(offsets);
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			getLogger().info("assigning new topic-partitions: {}", partitions);
		}
	};
}
