package jarvey.streams.updatelog;

import java.sql.Connection;

import utils.UsageHelp;
import utils.func.CheckedRunnable;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FormatKeyedUpdateIndexTable implements CheckedRunnable {
	@Mixin private UsageHelp m_help;
	@Mixin private JdbcParameters m_jdbcParams;
	
	@Option(names={"--table-name"}, paramLabel="name", required=true, description="table name")
	private String m_tableName;
	
	public void run() throws Exception {
		JdbcProcessor jdbc = m_jdbcParams.createJdbcProcessor();
		try ( Connection conn = jdbc.connect() ) {
			KeyedUpdateLogIndexBuilder.dropTable(conn, m_tableName);
			KeyedUpdateLogIndexBuilder.createTable(conn, m_tableName);
		}
	}
	
	public static final void main(String... args) throws Exception {
		FormatKeyedUpdateIndexTable cmd = new FormatKeyedUpdateIndexTable();
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
