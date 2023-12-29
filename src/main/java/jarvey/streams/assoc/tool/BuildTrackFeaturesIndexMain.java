package jarvey.streams.assoc.tool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;

import utils.LoggerNameBuilder;
import utils.LoggerSettable;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.processor.AbstractKafkaTopicProcessorDriver;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;
import jarvey.streams.updatelog.KeyedUpdateLogIndexerBuilder;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name = "track_features_index",
		description = "build track_features_index table.")
public class BuildTrackFeaturesIndexMain extends JarveyStreamsCommand {
	private static final Logger s_logger = LoggerNameBuilder.from(BuildTrackFeaturesIndexMain.class)
																.dropSuffix(2)
																.append("build.track_features_index")
																.getLogger();
	private static final String DEFAULT_GROUP_ID = "build-track-features-index";
	private static final String DEFAULT_INDEX_TABLE_NAME = "track_features_index";
	private static final String DEFAULT_MAX_POLL_INTERVAL = "10s";

	@Mixin private KafkaTopicProcessorDriver m_kafkaProcessorDriver;
	
	public static final void main(String... args) throws Exception {
		BuildTrackFeaturesIndexMain cmd = new BuildTrackFeaturesIndexMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.run();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
	
	public BuildTrackFeaturesIndexMain() {
		setLogger(s_logger);
	}
	
	@Override
	public void run(MCMOTConfig configs) throws Exception {
		m_kafkaProcessorDriver.m_configs = configs;
//		m_kafkaProcessorDriver.getKafkaOptions().setMaxPollInterval(DEFAULT_MAX_POLL_INTERVAL);
		m_kafkaProcessorDriver.setLogger(s_logger);
		
		m_kafkaProcessorDriver.run();
	}
	
	private static class KafkaTopicProcessorDriver extends AbstractKafkaTopicProcessorDriver<String,byte[]>
														implements LoggerSettable {
		private MCMOTConfig m_configs;
		private KafkaTopicPartitionProcessor<String,byte[]> m_processor;
		
		KafkaTopicProcessorDriver() {
			super();
			
			setLogger(s_logger);
		}

		@Override
		protected String getDefaultGroupId() {
			return DEFAULT_GROUP_ID;
		}

		@Override
		protected Collection<String> getInputTopics() {
			return Collections.singleton(m_configs.getTrackFeaturesTopic());
		}

		@Override
		protected KafkaTopicPartitionProcessor<String,byte[]> allocateProcessor(TopicPartition tpart)
			throws IOException {
			if ( m_processor != null ) {
				return m_processor;
			}

			JdbcProcessor jdbc = m_configs.getJdbcProcessor();
			String idxTableName = m_configs.getTableName("track-features")
											.getOrElse(DEFAULT_INDEX_TABLE_NAME);
			Deserializer<TrackFeature> deser = JarveySerdes.TrackFeature().deserializer();

			KeyedUpdateLogIndexerBuilder<TrackFeature> builder
				= new KeyedUpdateLogIndexerBuilder<>(idxTableName, deser, jdbc);
			return m_processor = KafkaTopicPartitionProcessor.wrap(builder);
		}
	}
}
