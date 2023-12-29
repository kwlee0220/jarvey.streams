package jarvey.streams.assoc.tool;

import java.sql.Connection;

import org.slf4j.Logger;

import utils.LoggerNameBuilder;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.assoc.AssociationStore;
import jarvey.streams.assoc.MCMOTConfig;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name = "associations",
		description = "format associations table.")
public class FormatAssociationStore extends JarveyStreamsCommand {
	private static final Logger s_logger = LoggerNameBuilder.from(FormatAssociationStore.class)
																.dropSuffix(2)
																.append("format.associations")
																.getLogger();
	
	@Override
	public void run(MCMOTConfig configs) throws Exception {
		JdbcProcessor jdbc = configs.getJdbcProcessor();
		try ( Connection conn = jdbc.connect() ) {
			AssociationStore.dropTable(conn);
			AssociationStore.createTable(conn);
		}
	}
	
	public static final void main(String... args) throws Exception {
		FormatAssociationStore cmd = new FormatAssociationStore();
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
}
