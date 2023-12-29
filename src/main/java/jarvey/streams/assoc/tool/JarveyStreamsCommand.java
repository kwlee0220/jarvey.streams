package jarvey.streams.assoc.tool;

import java.nio.file.Path;
import java.nio.file.Paths;

import utils.func.FOption;

import jarvey.streams.assoc.MCMOTConfig;

import picocli.CommandLine.Option;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class JarveyStreamsCommand extends HomeDirPicocliCommand {
	private static final String ENVVAR_HOME = "JARVEY_STREAMS_HOME";
	
	@Option(names={"--config"}, paramLabel="path", description={"JarveyStreams configuration file path"})
	protected String m_configPath = "mcmot_configs.yaml";
	
	abstract protected void run(MCMOTConfig configs) throws Exception;
	
	public JarveyStreamsCommand() {
		super(FOption.of(ENVVAR_HOME));
	}
	
	@Override
	protected final void run(Path homeDir) throws Exception {
		Path configPath = Paths.get(m_configPath);
		if ( !configPath.isAbsolute() ) {
			configPath = homeDir.resolve(configPath);
		}
		
		run(MCMOTConfig.load(configPath));
	}
}
