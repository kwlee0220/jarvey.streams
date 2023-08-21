package jarvey.streams.updatelog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.kafka.streams.kstream.Produced;
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
public final class KeyedUpdateLogIndexBuilder<T extends KeyedUpdate> implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(KeyedUpdateLogIndexBuilder.class);
	
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	private static final Duration DEFAULT_TTL = Duration.ofMinutes(10);
	private static final String STORE_INDEXES = "index";
	private static final Serde<KeyedUpdateIndex> SERDE_INDEX = GsonUtils.getSerde(KeyedUpdateIndex.class);
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	@Mixin private KafkaParameters m_kafkaParams;
	@Mixin private JdbcParameters m_jdbcParams;

	private String m_inputTopic;
	private String m_updateDelimitTopic;
	private String m_indexTableName;

	@Option(names={"--ttl"}, paramLabel="interval", description="time to live")
	private Duration m_timeToLive = DEFAULT_TTL;

	@Option(names={"--use-mock-store"}, description="Use mocking state store (for test).")
	private boolean m_useMockStateStore = false;
	
	private Serde<T> m_keyedUpdateSerde;
	
	private JdbcProcessor m_jdbc;
	private String m_insertSql;	

	@Option(names={"--input"}, paramLabel="topic-name", description="input topic name")
	public KeyedUpdateLogIndexBuilder<T> setInputTopic(String topic) {
		m_inputTopic = topic;
		
		if ( m_updateDelimitTopic == null ) {
			m_updateDelimitTopic = topic + "-delimit";
		}
		
		if ( m_indexTableName == null ) {
			setIndexTableName(topic.replace('-', '_') + "_index");
		}
		
		return this;
	}	

	@Option(names={"--delimit"}, paramLabel="topic-name", description="update delimit topic name")
	public KeyedUpdateLogIndexBuilder<T> setUpdateDelimitTopic(String topic) {
		m_updateDelimitTopic = topic;
		return this;
	}

	@Option(names={"--index-table"}, paramLabel="table name", description="output index table name")
	public KeyedUpdateLogIndexBuilder<T> setIndexTableName(String tblName) {
		m_indexTableName = tblName;
		m_insertSql = String.format(SQL_INSERT_NODE_TRACK_INDEX, tblName);
		return this;
	}
	
	public KeyedUpdateLogIndexBuilder<T> setApplicationId(String id) {
		m_kafkaParams.setApplicationId(id);
		return this;
	}
	
	public KeyedUpdateLogIndexBuilder<T> useKeyedUpdateSerde(Serde<T> serde) {
		m_keyedUpdateSerde = serde;
		return this;
	}
	
	public KeyedUpdateLogIndexBuilder<T> setAutoOffsetReset(AutoOffsetReset reset) {
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
			if ( !admin.existsTopic(m_updateDelimitTopic) ) {
				s_logger.warn("update-delimiter topic('{}') is not present. create one", m_updateDelimitTopic);
				admin.createTopic(m_updateDelimitTopic, 1, 1);
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
			.flatTransformValues(() -> new KeyedUpdateLogDelimiter<>(STORE_INDEXES, m_timeToLive,
																	m_useMockStateStore),
								Named.as("generate-index"), storeNames)
			.to(m_updateDelimitTopic,
				Produced.with(Serdes.String(), SERDE_INDEX)
						.withName("produce-indexes"));
		
		builder
			.stream(m_updateDelimitTopic,
					Consumed.with(Serdes.String(), SERDE_INDEX)
							.withName("consume-delimiters")
							.withTimestampExtractor(TS_EXTRACTOR)
							.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()))
			.filter((k, idx) -> idx.isClosed(), Named.as("filter-last-update"))
			.foreach(this::export);
		
		return builder.build();
	}
	
	private void export(String nodeId, KeyedUpdateIndex index) {
		try ( Connection conn = m_jdbc.connect() ) {
			try ( PreparedStatement pstmt = conn.prepareStatement(m_insertSql) ) {
				fillInsertStatement(index, pstmt);
				pstmt.execute();
			}
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}

	private static final String SQL_INSERT_NODE_TRACK_INDEX
		= "insert into %s (key, partition, first_offset, last_offset, first_ts, last_ts, length) "
			+	"values (?, ?, ?,?, ?,?, ?) "
		+ "on conflict (key) "
		+ "do update set "
			+ "partition=?, "
			+ "first_offset=?, last_offset=?, "
			+ "first_ts=?, last_ts=?, "
			+ "length=?";
	private void fillInsertStatement(KeyedUpdateIndex index, PreparedStatement pstmt) throws SQLException {
		pstmt.setString(1, index.getKey());
		pstmt.setInt(2, index.getPartitionNumber());
		pstmt.setLong(3, index.getTopicOffsetRange().min());
		pstmt.setLong(4, index.getTopicOffsetRange().max());
		pstmt.setLong(5, index.getTimestampRange().min());
		pstmt.setLong(6, index.getTimestampRange().max());
		pstmt.setInt(7, index.getUpdateCount());
		
		pstmt.setInt(8, index.getPartitionNumber());
		pstmt.setLong(9, index.getTopicOffsetRange().min());
		pstmt.setLong(10, index.getTopicOffsetRange().max());
		pstmt.setLong(11, index.getTimestampRange().min());
		pstmt.setLong(12, index.getTimestampRange().max());
		pstmt.setInt(13, index.getUpdateCount());
	}
	
	public static void createTable(Connection conn, String indexTableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(String.format(SQL_CREATE_TABLE, indexTableName));
	}
	
	public static void dropTable(Connection conn, String indexTableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("drop table if exists " + indexTableName);
	}
	
	private static final String SQL_CREATE_TABLE
		= "create table %s ("
		+ 	"key varchar not null, "
		+ 	"partition integer not null, "
		+ 	"first_offset bigint not null, "
		+ 	"last_offset bigint not null, "
		+ 	"first_ts bigint not null, "
		+ 	"last_ts bigint not null, "
		+ 	"length integer not null, "
		+ 	"primary key (key)"
		+ ")";
}
