package jarvey.streams;

import static utils.Utilities.checkArgument;
import static utils.Utilities.checkNotNullArgument;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

import utils.UnitUtils;
import utils.func.FOption;

import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KafkaOptions {
	private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
	
	private static final Serde<String> DEFAULT_KEY_SERDE = Serdes.String();
	private static final Serde<byte[]> DEFAULT_VALUE_SERDE = Serdes.ByteArray();
	
	private FOption<String> m_bootstrapServers = FOption.empty();
	private String m_autoOffset = "latest";
	private boolean m_enableAutoCommit = false;
	
	// 명시적으로 지정된 경우 해당 기간동안 kafka client poll()를 호출하지 않으면
	// Kafka broker는 해당 client가 단절되었다고 가정하고 rebalancing을 시도한다.
	// 기본값은 5분임.
	private FOption<Integer> m_maxPollIntervalMs = FOption.empty();
	// 컨슈머가 최대로 가져 갈 수있는 갯수.
	// 기본값은 500임.
	private FOption<Integer> m_maxPollRecords = FOption.empty();
	
	public FOption<String> getBootstrapServers() {
		return m_bootstrapServers;
	}
	public String getBootstrapServersOrDefault() {
		return m_bootstrapServers.getOrElse(DEFAULT_BOOTSTRAP_SERVERS);
	}
	
	@Option(names={"--bootstrap_servers"},
			paramLabel="kafka boostrap servers",
			description={"Kafka bootstrap servers, (default: localhost:9092)"})
	public void setBootstrapServers(String servers) {
		m_bootstrapServers = FOption.ofNullable(servers);
	}
	public void setBootstrapServersIfAbscent(String servers) {
		checkNotNullArgument(servers);
		m_bootstrapServers = FOption.of(servers);
	}

	public AutoOffsetReset getKafkaOfffset() {
		return AutoOffsetReset.valueOf(m_autoOffset.toUpperCase());
	}
	
	@Option(names={"--kafka_offset"}, paramLabel="offset")
	public void setKafkaOffset(String offsetStr) {
		m_autoOffset = offsetStr;
	}
	
	public boolean getEnableAutoCommit() {
		return m_enableAutoCommit;
	}
	
	@Option(names={"--enable_auto_commit"})
	public void setEnableAutoCommit(boolean flag) {
		m_enableAutoCommit = flag;
	}

	public FOption<Integer> getMaxPollInterval() {
		return m_maxPollIntervalMs;
	}
	
	@Option(names={"--max_poll_interval"}, paramLabel="interval")
	public void setMaxPollInterval(String intvlStr) {
		m_maxPollIntervalMs = FOption.of((int)UnitUtils.parseDurationMillis(intvlStr));
	}

	public FOption<Integer> getMaxPollRecords() {
		return m_maxPollRecords;
	}
	
	@Option(names={"--max_poll_records"}, paramLabel="count")
	public void setMaxPollRecords(int count) {
		checkArgument(count > 0, "invalid value: " + count);
		m_maxPollRecords = FOption.of(count);
	}
	
	public Properties toProducerProperties() {
		Properties props = new Properties();
		
		try {
			String bootstrapServers = m_bootstrapServers.getOrElse(DEFAULT_BOOTSTRAP_SERVERS);
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERDE.serializer().getClass().getName());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERDE.serializer().getClass().getName());
			
			return props;
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}

	public Properties toConsumerProperties() {
		Properties props = new Properties();
		
		try {
			String bootstrapServers = m_bootstrapServers.getOrElse(DEFAULT_BOOTSTRAP_SERVERS);
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, m_autoOffset);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, m_enableAutoCommit);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERDE.deserializer().getClass().getName());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERDE.deserializer().getClass().getName());
			m_maxPollIntervalMs.ifPresent(mills -> props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, mills));
			m_maxPollRecords.ifPresent(cnt -> props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cnt));
			
			return props;
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}
	
	public Properties toStreamProperties() {
		Properties props = new Properties();

		String bootstrapServers = m_bootstrapServers.getOrElse(DEFAULT_BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, DEFAULT_KEY_SERDE.getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, DEFAULT_VALUE_SERDE.getClass().getName());
		m_maxPollIntervalMs.ifPresent(mills -> props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, mills));
		m_maxPollRecords.ifPresent(cnt -> props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cnt));
		
		return props;
	}
	
	public void deleteConsumerGroup(String grpId) throws InterruptedException, ExecutionException {
		String bootstrapServers = getBootstrapServersOrDefault();
		deleteConsumerGroup(bootstrapServers, grpId);
	}
	
	public static void deleteConsumerGroup(String bootstrapServer, String grpId)
		throws InterruptedException, ExecutionException {
		KafkaAdmins admin = new KafkaAdmins(bootstrapServer);
		admin.deleteConsumerGroups(Collections.singletonList(grpId));
	}
}
