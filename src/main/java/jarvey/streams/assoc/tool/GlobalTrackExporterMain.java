package jarvey.streams.assoc.tool;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Lists;

import utils.UnitUtils;
import utils.UsageHelp;
import utils.func.CheckedRunnable;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.streams.assoc.AssociationStore;
import jarvey.streams.assoc.GlobalTrackExporter;
import jarvey.streams.assoc.motion.OverlapAreaRegistry;
import jarvey.streams.processor.AbstractKafkaTopicProcessorDriver;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="global-track-exporter",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Export global tracks into JDBC-aware DBMS")
public class GlobalTrackExporterMain extends AbstractKafkaTopicProcessorDriver<String,byte[]>
											implements CheckedRunnable {
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Mixin private JdbcParameters m_jdbcParams;

	private List<String> m_inputTopics = Lists.newArrayList("node-tracks");
	
	@Option(names={"--exporter-jdbc-url"},
			paramLabel="<system>:<jdbc_host>:<jdbc_port>:<user_id>:<passwd>:<db_name>",
			required=true,
			description={"JDBC locator for expoting database, (eg. 'mysql:localhost:3306:sbdata:xxxyy:bigdata')"})
	private String m_exportJdbcUrl = "kairos://129.254.82.161:5000:root:root/DNA";
	
	private Duration m_runningTime;
	
	private String m_overlapAreaFilePath = "overlap_areas.yaml";
	
	private OverlapAreaRegistry m_areaRegistry = null;

	@Option(names={"--overlap-area"}, paramLabel="overlap-area-descriptor",
			description="overlap area description file path.")
	public void setOverlapAreaFile(String path) {
		m_overlapAreaFilePath = path;
	}
	
	@Override
	protected Collection<String> getInputTopics() {
		return m_inputTopics;
	}
	@Option(names={"--input"}, paramLabel="topic names", description="input topic names")
	public void setInputTopics(String names) {
		m_inputTopics = FStream.of(names.split(",")).map(String::trim).toList();
	}

	@Option(names={"--running-time"}, paramLabel="duration", description="running time")
	public void setRunningTime(String durStr) {
		m_runningTime = UnitUtils.parseDuration(durStr);
	}

	@Override
	protected KafkaTopicPartitionProcessor<String,byte[]> allocateProcessor(TopicPartition tpart)
		throws IOException, SQLException {
		if ( m_areaRegistry == null ) {
			m_areaRegistry = OverlapAreaRegistry.load(new File(m_overlapAreaFilePath));
		}
		
		JdbcProcessor jdbc = m_jdbcParams.createJdbcProcessor();
		AssociationStore assocStore = new AssociationStore(jdbc);
		
		JdbcParameters exporterJdbcParams = new JdbcParameters();
		exporterJdbcParams.jdbcLoc(m_exportJdbcUrl);
		JdbcProcessor exportJdbc = exporterJdbcParams.createJdbcProcessor();

		GlobalTrackExporter gen = new GlobalTrackExporter(m_areaRegistry, assocStore, exportJdbc, m_runningTime);
		return KafkaTopicPartitionProcessor.wrap(gen);
	}
	
	public static final void main(String... args) throws Exception {
		GlobalTrackExporterMain cmd = new GlobalTrackExporterMain();
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
}
