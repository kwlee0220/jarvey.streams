package jarvey.streams.assoc.tool;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import utils.LoggerNameBuilder;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.assoc.BuildGlobalTrajectories;
import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.processor.AbstractKafkaTopicProcessorDriver;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="trajectories",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Export global tracks into JDBC-aware DBMS")
public class BuildGlobalTrajectoriesMain extends JarveyStreamsCommand {
	private static final Logger s_logger = LoggerNameBuilder.from(BuildGlobalTrajectoriesMain.class)
																.dropSuffix(2)
																.append("build.trajectories")
																.getLogger();
	
	private static final String DEFAULT_GROUP_ID = "export-trajectories";
	private static final String DEFAULT_MAX_POLL_INTERVAL = "10s";
	
	@Mixin private KafkaTopicProcessorDriver m_kafkaProcessorDriver;
	
	public static final void main(String... args) throws Exception {
		BuildGlobalTrajectoriesMain cmd = new BuildGlobalTrajectoriesMain();
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
	
	public BuildGlobalTrajectoriesMain() {
		setLogger(s_logger);
	}
	
	@Override
	public void run(MCMOTConfig configs) throws Exception {
		m_kafkaProcessorDriver.m_configs = configs;
//		m_kafkaProcessorDriver.getKafkaOptions().setMaxPollInterval(DEFAULT_MAX_POLL_INTERVAL);
		m_kafkaProcessorDriver.setLogger(s_logger);
		
		m_kafkaProcessorDriver.run();
	}
	
	private static class KafkaTopicProcessorDriver extends AbstractKafkaTopicProcessorDriver<String,byte[]> {
		private MCMOTConfig m_configs;
		private KafkaTopicPartitionProcessor<String,byte[]> m_processor;
		
		@Option(names={"--exporter-jdbc-url"},
				paramLabel="<system>:<jdbc_host>:<jdbc_port>:<user_id>:<passwd>:<db_name>",
				required=true,
				description={"JDBC locator for expoting database, (eg. 'mysql:localhost:3306:sbdata:xxxyy:bigdata')"})
		private String m_exportJdbcUrl = "kairos://129.254.82.161:5000:root:root/DNA";
		
		@Override
		protected String getDefaultGroupId() {
			return DEFAULT_GROUP_ID;
		}

		@Override
		protected Collection<String> getInputTopics() {
			return Collections.singleton(m_configs.getGlobalTracksTopic());
		}

		@Override
		protected KafkaTopicPartitionProcessor<String,byte[]> allocateProcessor(TopicPartition tpart)
			throws IOException, SQLException {
			JdbcParameters exporterJdbcParams = new JdbcParameters();
			exporterJdbcParams.jdbcLoc(m_exportJdbcUrl);
			JdbcProcessor exportJdbc = exporterJdbcParams.createJdbcProcessor();

			BuildGlobalTrajectories gen = new BuildGlobalTrajectories(exportJdbc);
			return KafkaTopicPartitionProcessor.wrap(gen);
		}
	}
}
