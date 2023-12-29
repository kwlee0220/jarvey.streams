package jarvey.streams.assoc.tool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import utils.LoggerNameBuilder;
import utils.LoggerSettable;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.assoc.AssociationCache;
import jarvey.streams.assoc.AssociationStore;
import jarvey.streams.assoc.GlobalTrackGenerator;
import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.assoc.ReactiveAssociationCache;
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
@Command(name="global_tracks",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Generate global tracks with final associations.")
public class BuildGlobalTracksMain extends JarveyStreamsCommand {
	private static final Logger s_logger = LoggerNameBuilder.from(BuildGlobalTracksMain.class)
																.dropSuffix(3)
																.append("global_tracks")
																.getLogger();
	
	private static final String DEFAULT_GROUP_ID = "build-global-tracks";
	
	@Mixin private KafkaTopicProcessorDriver m_kafkaProcessorDriver;
	
	public BuildGlobalTracksMain() {
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
		BuildGlobalTracksMain cmd = new BuildGlobalTracksMain();
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
	
	private static class KafkaTopicProcessorDriver extends AbstractKafkaTopicProcessorDriver<String,byte[]> 
														implements LoggerSettable {
		private MCMOTConfig m_configs;
		private ReactiveAssociationCache m_assocCache;
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
			return Arrays.asList(m_configs.getNodeTracksTopic(), m_configs.getTrackletAssociatedsTopic());
		}

		@Override
		protected KafkaTopicPartitionProcessor<String,byte[]> allocateProcessor(TopicPartition tpart)
			throws IOException {
			
			if ( tpart.topic().equals(m_configs.getNodeTracksTopic()) ) {
				if ( m_processor == null ) {
					Properties producerProps = m_kafkaOptions.toProducerProperties();
					KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

					ReactiveAssociationCache cache = createAssociationCache();
					GlobalTrackGenerator gen = new GlobalTrackGenerator(m_configs, cache, producer);
					gen.setLogger(getLogger());
					m_processor = KafkaTopicPartitionProcessor.wrap(gen);
				}
				return m_processor;
			}
			else if ( tpart.topic().equals(m_configs.getTrackletAssociatedsTopic()) ) {
				return createAssociationCache();
			}
			else {
				throw new IllegalArgumentException("invalid topic: " + tpart.topic());
			}
		}
		
		private ReactiveAssociationCache createAssociationCache() {
			if ( m_assocCache == null ) {
				JdbcProcessor jdbc = m_configs.getJdbcProcessor();
				AssociationStore assocStore = new AssociationStore(jdbc);
				int cacheSize = m_configs.getOutputConfigs().getCacheSize();
				m_assocCache = new ReactiveAssociationCache(new AssociationCache(cacheSize, assocStore));
				m_assocCache.setLogger(LoggerNameBuilder.plus(getLogger(), "cache"));
			}
			
			return m_assocCache;
		}
	}
}
