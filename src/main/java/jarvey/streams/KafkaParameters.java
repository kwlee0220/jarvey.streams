package jarvey.streams;

import static utils.Utilities.checkArgument;

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
public class KafkaParameters {
	private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
	
	private FOption<String> m_oclientId = FOption.empty();
	private FOption<String> m_ogroupId = FOption.empty();
	private FOption<String> m_oappId = FOption.empty();
	
	private FOption<String> m_bootstrapServers = FOption.empty();
	private String m_autoOffset = "latest";
	private boolean m_enableAutoCommit = false;
	
	private String m_keySerdeClassName = Serdes.String().getClass().getName();
	private String m_valueSerdeClassName = Serdes.ByteArray().getClass().getName();
	
	// 명시적으로 지정된 경우 해당 기간동안 kafka client poll()를 호출하지 않으면
	// Kafka broker는 해당 client가 단절되었다고 가정하고 rebalancing을 시도한다.
	// 기본값은 5분임.
	private FOption<Long> m_maxPollIntervalMs = FOption.empty();
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

	public FOption<String> getClientId() {
		return m_oclientId;
	}
	
	@Option(names={"--client-id"}, paramLabel="id", description={"Kafka client id"})
	public void setClientId(String clientId) {
		m_oclientId = FOption.ofNullable(clientId);
	}

	public FOption<String> getGroupId() {
		return m_ogroupId;
	}
	
	@Option(names={"--group_id"}, paramLabel="id", description={"Kafka group id"})
	public void setGroupId(String grpId) {
		m_ogroupId = FOption.ofNullable(grpId);
	}

	public FOption<String> getApplicationId() {
		return m_oappId;
	}
	
	@Option(names={"--app_id"}, paramLabel="id", description={"KafkaStreams application id"})
	public void setApplicationId(String appId) {
		m_oappId = FOption.ofNullable(appId);
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

	public String getKeySerde() {
		return m_keySerdeClassName;
	}
	
	@Option(names={"--key_serde"}, paramLabel="class name", description="default: Serdes.String")
	public void setKeySerde(String className) {
		m_keySerdeClassName = className;
	}

	public String getValueSerde() {
		return m_valueSerdeClassName;
	}
	
	@Option(names={"--value_serde"}, paramLabel="class name",
			description="default: org.apache.kafka.common.serialization.ByteArraySerializer")
	public void setValueSerde(String className) {
		m_valueSerdeClassName = className;
	}

	public FOption<Long> getMaxPollInterval() {
		return m_maxPollIntervalMs;
	}
	
	@Option(names={"--max_poll_interval"}, paramLabel="interval")
	public void setMaxPollInterval(String intvlStr) {
		m_maxPollIntervalMs = FOption.of(UnitUtils.parseDurationMillis(intvlStr));
	}

	public FOption<Integer> getMaxPollRecords() {
		return m_maxPollRecords;
	}
	
	@Option(names={"--max_poll_records"}, paramLabel="count")
	public void setMaxPollRecords(int count) {
		checkArgument(count > 0, "invalid value: " + count);
		m_maxPollRecords = FOption.of(count);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Properties toProducerProperties() {
		Properties props = new Properties();
		
		try {
			String keySerializerClassName;
			if ( m_keySerdeClassName == null ) {
				keySerializerClassName = Serdes.String().serializer().getClass().getName();
			}
			else {
				Class<Serde> serdeCls = (Class<Serde>)Class.forName(m_keySerdeClassName);
				keySerializerClassName = serdeCls.newInstance().serializer().getClass().getName();
			}
			
			String valueSerializerClassName;
			if ( m_valueSerdeClassName == null ) {
				valueSerializerClassName = Serdes.ByteArray().serializer().getClass().getName();
			}
			else {
				Class<Serde> serdeCls = (Class<Serde>)Class.forName(m_valueSerdeClassName);
				valueSerializerClassName = serdeCls.newInstance().serializer().getClass().getName();
			}
			
			String bootstrapServers = m_bootstrapServers.getOrElse(DEFAULT_BOOTSTRAP_SERVERS);
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
			
			return props;
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Properties toConsumerProperties() {
		Properties props = new Properties();
		
		try {
			String keyDeserializerClassName;
			if ( m_keySerdeClassName == null ) {
				keyDeserializerClassName = Serdes.String().deserializer().getClass().getName();
			}
			else {
				Class<Serde> serdeCls = (Class<Serde>)Class.forName(m_keySerdeClassName);
				keyDeserializerClassName = serdeCls.newInstance().deserializer().getClass().getName();
			}
			
			String valueDeserializerClassName;
			if ( m_valueSerdeClassName == null ) {
				valueDeserializerClassName = Serdes.ByteArray().serializer().getClass().getName();
			}
			else {
				Class<Serde> serdeCls = (Class<Serde>)Class.forName(m_valueSerdeClassName);
				valueDeserializerClassName = serdeCls.newInstance().deserializer().getClass().getName();
			}

			String bootstrapServers = m_bootstrapServers.getOrElse(DEFAULT_BOOTSTRAP_SERVERS);
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

			m_ogroupId
				.ifPresent(id -> props.put(ConsumerConfig.GROUP_ID_CONFIG, id))
				.ifAbsent(() -> {
					String id = m_oclientId.getOrElse(() -> Long.toHexString(System.nanoTime()));
					props.put(ConsumerConfig.GROUP_ID_CONFIG, id);
				});
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, m_autoOffset);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, m_enableAutoCommit);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
			
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
		String appId = m_oappId.orElse(m_ogroupId)
								.orElse(m_oclientId)
								.getOrElse(() -> Long.toHexString(System.nanoTime()));
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, m_keySerdeClassName);
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, m_valueSerdeClassName);
		m_maxPollIntervalMs.ifPresent(mills -> props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, mills));
		m_maxPollRecords.ifPresent(cnt -> props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cnt));
		
		return props;
	}
	
	public void deleteConsumerGroup() throws InterruptedException, ExecutionException {
		FOption<String> ogrpId = getGroupId();
		if ( ogrpId.isPresent() ) {
			String bootstrapServers = m_bootstrapServers.getOrElse(DEFAULT_BOOTSTRAP_SERVERS);
			deleteConsumerGroup(bootstrapServers, ogrpId.get());
		}
		else {
			throw new IllegalArgumentException("client-id is not specified");
		}
	}
	
	public static void deleteConsumerGroup(String bootstrapServer, String grpId)
		throws InterruptedException, ExecutionException {
		KafkaAdmins admin = new KafkaAdmins(bootstrapServer);
		admin.deleteConsumerGroups(Collections.singletonList(grpId));
	}
}
