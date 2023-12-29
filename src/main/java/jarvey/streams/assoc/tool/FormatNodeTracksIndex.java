package jarvey.streams.assoc.tool;

import java.sql.Connection;

import org.slf4j.Logger;

import utils.LoggerNameBuilder;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.node.NodeTrackIndexBuilder;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name = "node_tracks_index",
		description = "format node-tracks-index table.")
public class FormatNodeTracksIndex extends JarveyStreamsCommand {
	private static final Logger s_logger = LoggerNameBuilder.from(FormatNodeTracksIndex.class)
																.dropSuffix(2)
																.append("format.node_tracks_index")
																.getLogger();
	
	private static final String DEFAULT_INDEX_TABLE_NAME = "node_tracks_index";
	
	@Option(names={"--table-name"}, paramLabel="name", defaultValue=DEFAULT_INDEX_TABLE_NAME,
			description="table name")
	private String m_tableName;
	
	public FormatNodeTracksIndex() {
		setLogger(s_logger);
	}
	
	@Override
	public void run(MCMOTConfig configs) throws Exception {
		JdbcProcessor jdbc = configs.getJdbcProcessor();
		try ( Connection conn = jdbc.connect() ) {
			NodeTrackIndexBuilder.dropIndexTable(conn, m_tableName);
			NodeTrackIndexBuilder.createIndexTable(conn, m_tableName);
		}
	}
	
	public static final void main(String... args) throws Exception {
		FormatNodeTracksIndex cmd = new FormatNodeTracksIndex();
		
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
