package jarvey.streams;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

import utils.UnitUtils;

import picocli.CommandLine.Option;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KafkaParameters {
	private String m_bootstrapServers = "localhost:9092";
	private String m_appId = null;
	private String m_autoOffset = "latest";
	private boolean m_enableAutoCommit = false;
	
	private Duration m_pollTimeout = Duration.ofSeconds(3);
	private Duration m_initialPollTimeout = Duration.ofSeconds(10);
	
//	private String m_keySerializer = StringSerializer.class.getName();
//	private String m_valueSerializer = ByteArraySerializer.class.getName();
//	private String m_keyDeserializer = StringDeserializer.class.getName();
//	private String m_valueDeserializer = ByteArrayDeserializer.class.getName();
	private String m_keySerdeClassName = Serdes.String().getClass().getName();
	private String m_valueSerdeClassName = Serdes.ByteArray().getClass().getName();
	
	private long m_maxPollIntervalMs = -1;
	private int m_maxPollRecords = -1;
	
	public String getBootstrapServers() {
		return m_bootstrapServers;
	}
	
	@Option(names={"--bootstrap-servers"},
			paramLabel="kafka boostrap servers",
			description={"Kafka bootstrap servers, (default: localhost:9092)"})
	public void setBootstrapServers(String servers) {
		m_bootstrapServers = servers;
	}

	public String getApplicationId() {
		return m_appId;
	}
	
	@Option(names={"--app-id"}, paramLabel="application id")
	public void setApplicationId(String appId) {
		m_appId = appId;
	}

	public AutoOffsetReset getAutoOffsetReset() {
		return AutoOffsetReset.valueOf(m_autoOffset.toUpperCase());
	}
	
	@Option(names={"--auto-offset-reset"}, paramLabel="offset")
	public void setAutoOffsetReset(String offsetStr) {
		m_autoOffset = offsetStr;
	}
	
	public boolean getEnableAutoCommit() {
		return m_enableAutoCommit;
	}
	
	@Option(names={"--enable-auto-commit"})
	public void setEnableAutoCommit(boolean flag) {
		m_enableAutoCommit = flag;
	}

	public Duration getPollTimeout() {
		return m_pollTimeout;
	}
	@Option(names={"--poll-timeout"}, paramLabel="duration", description="default: '3s'")
	public void setPollTimeout(String durStr) {
		m_pollTimeout = Duration.ofMillis(UnitUtils.parseDuration(durStr));
	}

	public Duration getInitialPollTimeout() {
		return m_initialPollTimeout;
	}
	@Option(names={"--initial-poll-timeout"}, paramLabel="duration", description="default: '10s'")
	public void setInitialPollTimeout(String durStr) {
		m_initialPollTimeout = Duration.ofMillis(UnitUtils.parseDuration(durStr));
	}

	public String getKeySerde() {
		return m_keySerdeClassName;
	}
	
	@Option(names={"--key-serde"}, paramLabel="class name", description="default: Serdes.String")
	public void setKeySerde(String className) {
		m_keySerdeClassName = className;
	}

	public String getValueSerde() {
		return m_valueSerdeClassName;
	}
	
	@Option(names={"--value-serde"}, paramLabel="class name",
			description="default: org.apache.kafka.common.serialization.ByteArraySerializer")
	public void setValueSerde(String className) {
		m_valueSerdeClassName = className;
	}
	
//	@Option(names={"--key-deserializer"}, paramLabel="class name",
//			description="default: org.apache.kafka.common.serialization.StringDeserializer")
//	public void setKeyDeserializer(String className) {
//		m_keyDeserializer = className;
//	}
//
//	public String getKeyDeserializer() {
//		return m_keyDeserializer;
//	}
//	
//	@Option(names={"--key-deserializer"}, paramLabel="class name",
//			description="default: org.apache.kafka.common.serialization.StringDeserializer")
//	public void setKeyDeserializer(String className) {
//		m_keyDeserializer = className;
//	}
//
//	public String getValueDeserializer() {
//		return m_valueDeserializer;
//	}
//	
//	@Option(names={"--value-deserializer"}, paramLabel="class name",
//			description="default: org.apache.kafka.common.serialization.ByteArrayDeserializer")
//	public void setValueDeserializer(String className) {
//		m_valueDeserializer = className;
//	}
//
//	public String getKeySerializer() {
//		return m_keySerializer;
//	}
//	
//	@Option(names={"--key-serializer"}, paramLabel="class name",
//			description="default: org.apache.kafka.common.serialization.StringSerializer")
//	public void setKeySerializer(String className) {
//		m_keySerializer = className;
//	}
//
//	public String getValueSerializer() {
//		return m_valueSerializer;
//	}
//	
//	@Option(names={"--value-serializer"}, paramLabel="class name",
//			description="default: org.apache.kafka.common.serialization.ByteArraySerializer")
//	public void setValueSerializer(String className) {
//		m_valueSerializer = className;
//	}

	public long getMaxPollInterval() {
		return m_maxPollIntervalMs;
	}
	
	@Option(names={"--max.poll.interval"}, paramLabel="interval")
	public void setMaxPollInterval(String intvlStr) {
		m_maxPollIntervalMs = UnitUtils.parseDuration(intvlStr);
	}

	public int getMaxPollRecords() {
		return m_maxPollRecords;
	}
	
	@Option(names={"--max.poll.records"}, paramLabel="count")
	public void setMaxPollRecords(int count) {
		m_maxPollRecords = count;
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
			
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, m_bootstrapServers);
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
			
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_bootstrapServers);
			if ( m_appId != null ) {
				props.put(ConsumerConfig.GROUP_ID_CONFIG, m_appId);
			}
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, m_autoOffset);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ""+m_enableAutoCommit);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
			
			if ( m_maxPollIntervalMs > 0 ) {
				props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, m_maxPollIntervalMs);
			}
			if ( m_maxPollRecords > 0 ) {
				props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, m_maxPollRecords);
			}
			
			return props;
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}
	
	public Properties toStreamProperties() {
		Properties props = new Properties();

		if ( m_appId != null ) {
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, m_appId);
		}
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, m_bootstrapServers);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, m_keySerdeClassName);
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, m_valueSerdeClassName);
		if ( m_maxPollRecords > 0 ) {
			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, m_maxPollRecords);
		}
		
		return props;
	}
}
