package jarvey.streams.assoc.tool;

import java.util.Collection;
import java.util.Collections;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import utils.LoggerNameBuilder;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.node.NodeTrackIndexBuilder;
import jarvey.streams.processor.AbstractKafkaTopicProcessorDriver;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name = "node_tracks_index",
		description = "build node-tracks-index table.")
public class BuildNodeTracksIndexMain extends JarveyStreamsCommand {
	private static final Logger s_logger = LoggerNameBuilder.from(BuildNodeTracksIndexMain.class)
																.dropSuffix(2)
																.append("build.node_tracks_index")
																.getLogger();
	
	private static final String DEFAULT_GROUP_ID = "build-node-track-index";
	private static final String DEFAULT_INDEX_TABLE_NAME = "node_tracks_index";
	
	@Mixin private KafkaTopicProcessorDriver m_kafkaProcessorDriver;
	
	private BuildNodeTracksIndexMain() {
		setLogger(s_logger);
	}
	
	@Override
	public void run(MCMOTConfig configs) throws Exception {
		m_kafkaProcessorDriver.m_configs = configs;
//		m_kafkaProcessorDriver.getKafkaOptions().setMaxPollInterval(DEFAULT_MAX_POLL_INTERVAL);
		m_kafkaProcessorDriver.setLogger(s_logger);
		m_kafkaProcessorDriver.run();
	}
	
	public static final void main(String... args) throws Exception {
		BuildNodeTracksIndexMain cmd = new BuildNodeTracksIndexMain();
		cmd.setLogger(s_logger);
		
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
	
	private static class KafkaTopicProcessorDriver extends AbstractKafkaTopicProcessorDriver<String,byte[]> {
		private MCMOTConfig m_configs;
		private KafkaTopicPartitionProcessor<String,byte[]> m_processor;
		
		KafkaTopicProcessorDriver() {
			setLogger(s_logger);
		}

		@Override
		protected String getDefaultGroupId() {
			return DEFAULT_GROUP_ID;
		}

		@Override
		protected Collection<String> getInputTopics() {
			return Collections.singleton(m_configs.getNodeTracksTopic());
		}
		
		@Override
		protected KafkaTopicPartitionProcessor<String,byte[]> allocateProcessor(TopicPartition tpart)
			throws Exception {
			if ( m_processor != null ) {
				return m_processor;
			}
			
			JdbcProcessor jdbc = m_configs.getJdbcProcessor();
			String idxTableName = m_configs.getTableName("node-tracks")
											.getOrElse(DEFAULT_INDEX_TABLE_NAME);
			NodeTrackIndexBuilder builder = new NodeTrackIndexBuilder(idxTableName, jdbc);
			builder.setLogger(getLogger());
			return m_processor = KafkaTopicPartitionProcessor.wrap(builder);
		}
	}
}
