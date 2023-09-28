package jarvey.streams.assoc.tool;

import org.apache.kafka.common.serialization.Serde;

import jarvey.streams.model.TrackFeatureSerde;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.updatelog.KeyedUpdateLogIndexerBuilder;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTrackFeatureIndexMain {
	public static final void main(String... args) throws Exception {
		KeyedUpdateLogIndexerBuilder<TrackFeature> cmd = new KeyedUpdateLogIndexerBuilder<>();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				Serde<TrackFeature> serde = TrackFeatureSerde.s_serde;
				
				cmd.setApplicationId("track-features-indexer")
					.setInputTopic("track-features")
					.setIndexTableName("track_features_index")
					.useKeyedUpdateSerde(serde);
				
				cmd.run();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
