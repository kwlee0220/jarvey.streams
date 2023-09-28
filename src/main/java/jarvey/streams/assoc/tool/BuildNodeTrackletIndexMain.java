package jarvey.streams.assoc.tool;

import jarvey.streams.node.NodeTrackletIndexCreator;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildNodeTrackletIndexMain {
	public static final void main(String... args) throws Exception {
		NodeTrackletIndexCreator cmd = new NodeTrackletIndexCreator();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.setApplicationId("node-tracklet-indexer")
					.setInputTopic("node-tracks");
				
				cmd.run();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
