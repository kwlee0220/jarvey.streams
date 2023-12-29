package jarvey.streams.updatelog;

import java.sql.Connection;
import java.util.concurrent.Callable;

import utils.UsageHelp;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name = "keyed_updates",
		description = "format keyed-update-index table.")
public class FormatKeyedUpdateIndex implements Callable<Void> {
	@Mixin private UsageHelp m_help;
	@Mixin private JdbcParameters m_jdbcParams;
	
	@Option(names={"--table-name"}, paramLabel="name", required=true, description="table name")
	private String m_tableName;
	
	public Void call() throws Exception {
		JdbcProcessor jdbc = m_jdbcParams.createJdbcProcessor();
		try ( Connection conn = jdbc.connect() ) {
			KeyedUpdateIndexBuilder.dropTable(conn, m_tableName);
			KeyedUpdateIndexBuilder.createTable(conn, m_tableName);
		}
		
		return null;
	}
	
	public static final void main(String... args) throws Exception {
		FormatKeyedUpdateIndex cmd = new FormatKeyedUpdateIndex();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.call();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
