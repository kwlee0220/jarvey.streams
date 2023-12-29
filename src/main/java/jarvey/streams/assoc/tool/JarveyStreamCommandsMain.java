package jarvey.streams.assoc.tool;

import utils.UsageHelp;

import jarvey.streams.assoc.tool.JarveyStreamCommandsMain.BuildCommand;
import jarvey.streams.assoc.tool.JarveyStreamCommandsMain.FormatCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Help.Ansi;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="jarvey_streams",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="JarveyStreams related commands",
		subcommands = {
			FormatCommand.class,
			BuildCommand.class,
		})
public class JarveyStreamCommandsMain implements Runnable {
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Override
	public void run() {
		m_spec.commandLine().usage(System.out, Ansi.OFF);
	}

	public static final void main(String... args) throws Exception {
		JarveyStreamCommandsMain cmd = new JarveyStreamCommandsMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}

	@Command(name="format",
			description="JarveyStreams format related commands",
			subcommands= {
				FormatAssociationStore.class,
				FormatNodeTracksIndex.class,
				FormatTrackFeaturesIndex.class,
			})
	public static class FormatCommand implements Runnable {
		@Spec private CommandSpec m_spec;
		@Mixin private UsageHelp m_help;
		
		@Override
		public void run() {
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}

	@Command(name="build",
			description="JarveyStreams build related commands",
			subcommands= {
				BuildNodeTracksIndexMain.class,
				BuildTrackFeaturesIndexMain.class,
				AssociateTrackletMain.class,
				BuildGlobalTracksMain.class,
				BuildGlobalTrajectoriesMain.class,
			})
	public static class BuildCommand implements Runnable {
		@Spec private CommandSpec m_spec;
		@Mixin private UsageHelp m_help;
		
		@Override
		public void run() {
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}
}
