package jarvey.streams.updatelog;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.UsageHelp;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.KafkaAdmins;
import jarvey.streams.KafkaParameters;
import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.serialization.json.GsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="keyedupdate-index-table-builder",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Build a index table for keyed-update logs")
public final class KeyedUpdateLogIndexerBuilder<T extends KeyedUpdate> implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(KeyedUpdateLogIndexerBuilder.class);
	
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	private static final Duration DEFAULT_TTL = Duration.ofMinutes(10);
	private static final String STORE_INDEXES = "index";
	private static final Serde<KeyedUpdateIndex> SERDE_INDEX = GsonUtils.getSerde(KeyedUpdateIndex.class);
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	@Mixin private KafkaParameters m_kafkaParams;
	@Mixin private JdbcParameters m_jdbcParams;

	private String m_inputTopic;
	private String m_indexTableName;

	@Option(names={"--ttl"}, paramLabel="interval", description="time to live")
	private Duration m_timeToLive = DEFAULT_TTL;

	@Option(names={"--use-mock-store"}, description="Use mocking state store (for test).")
	private boolean m_useMockStateStore = false;
	
	private Serde<T> m_keyedUpdateSerde;
	private JdbcProcessor m_jdbc;

	@Option(names={"--input"}, paramLabel="topic-name", description="input topic name")
	public KeyedUpdateLogIndexerBuilder<T> setInputTopic(String topic) {
		m_inputTopic = topic;
		
		if ( m_indexTableName == null ) {
			setIndexTableName(topic.replace('-', '_') + "_index");
		}
		
		return this;
	}

	@Option(names={"--index-table"}, paramLabel="table name", description="output index table name")
	public KeyedUpdateLogIndexerBuilder<T> setIndexTableName(String tblName) {
		m_indexTableName = tblName;
		return this;
	}
	
	public KeyedUpdateLogIndexerBuilder<T> setApplicationId(String id) {
		m_kafkaParams.setApplicationId(id);
		return this;
	}
	
	public KeyedUpdateLogIndexerBuilder<T> useKeyedUpdateSerde(Serde<T> serde) {
		m_keyedUpdateSerde = serde;
		return this;
	}
	
	public KeyedUpdateLogIndexerBuilder<T> setAutoOffsetReset(AutoOffsetReset reset) {
		m_kafkaParams.setAutoOffsetReset(reset.toString());
		return this;
	}
	
	@Override
	public void run() {
		m_jdbc = m_jdbcParams.createJdbcProcessor();
		
		Topology topology = build();
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("use Kafka servers: {}", m_kafkaParams.getBootstrapServers());
			s_logger.info("use Kafka application: {}", m_kafkaParams.getApplicationId());
		}
		
		Properties props = m_kafkaParams.toStreamProperties();
		KafkaStreams streams = new KafkaStreams(topology, props);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
		streams.cleanUp();
		
		KafkaAdmins admin = new KafkaAdmins(m_kafkaParams.getBootstrapServers());
		try {
			if ( !admin.existsTopic(m_inputTopic) ) {
				s_logger.warn("input topic('{}') is not present. create one", m_inputTopic);
				admin.createTopic(m_inputTopic, 1, 1);
			}
		}
		catch ( Exception e ) {
			s_logger.warn("fails to get topic description", e);
		}
		streams.start();
	}
	
	public Topology build() {
		StreamsBuilder builder = new StreamsBuilder();

		String[] storeNames = {};
		if ( !m_useMockStateStore ) {
			storeNames = new String[] {STORE_INDEXES};
			builder.addStateStore(Stores.keyValueStoreBuilder(
														Stores.persistentKeyValueStore(STORE_INDEXES),
														Serdes.String(), SERDE_INDEX));
		}
		
		builder
			.stream(m_inputTopic,
					Consumed.with(Serdes.String(), m_keyedUpdateSerde)
							.withName("consume-updates")
							.withTimestampExtractor(TS_EXTRACTOR)
							.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()))
			.flatTransformValues(() -> new KeyedUpdateIndexBuilder<>(STORE_INDEXES, m_timeToLive, m_useMockStateStore,
																m_jdbc, m_indexTableName),
								Named.as("generate-index"), storeNames)
			.foreach((k,v) -> {});
		
		return builder.build();
	}
}
