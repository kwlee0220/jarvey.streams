package jarvey.streams.assoc.tool;

import java.io.File;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import utils.Throwables;
import utils.UsageHelp;

import jarvey.streams.DelayedTransformer;
import jarvey.streams.KafkaParameters;
import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.assoc.AssocSerdes;
import jarvey.streams.assoc.AssociationCollection;
import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.assoc.RunningGlobalTrackGenerator;
import jarvey.streams.assoc.feature.FeatureAssociationStreamBuilder;
import jarvey.streams.assoc.motion.MotionAssociationStreamBuilder;
import jarvey.streams.assoc.motion.OverlapAreaRegistry;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;

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
@Command(name="track-association",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="NodeTrack association")
final class AssociatorMain implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(AssociatorMain.class);

	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Option(names={"--config"}, paramLabel="path", description="configuration file path")
	private File m_configFile = new File("mcmot_configs.yaml");
	
	@Option(names={"--skip-motion"}, description="skip motion-based association")
	private boolean m_skipMotion = false;
	
	@Option(names={"--skip-feature"}, description="skip feature-based association")
	private boolean m_skipFeature = false;
	
	@Option(names={"--skip-global-tracks"}, description="skip global-tracks generation")
	private boolean m_skipGlobalTracks = false;
	
	@Option(names={"--cleanup"}, description="clean-up Kafka application.")
	private boolean m_cleanUp = false;
	
	private MCMOTConfig m_configs;
	private KafkaParameters m_kafkaParams;
	
	@Override
	public void run() {
		try {
			m_configs = MCMOTConfig.load(m_configFile);
			
			m_kafkaParams = m_configs.getKafkaParameters();
			AssociationCollection associations = new AssociationCollection("associations");
			Set<TrackletId> closedTracklets = Sets.newHashSet();
			AssociationCollection finalAssociations = new AssociationCollection("final-associations");
			
			StreamsBuilder builder = new StreamsBuilder();
			KStream<String,NodeTrack> nodeTracks =
				builder
					.stream(m_configs.getNodeTracksTopic(),
							Consumed.with(Serdes.String(), JarveySerdes.NodeTrack())
									.withName("from-node-tracks")
									.withTimestampExtractor(TS_EXTRACTOR)
									.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()));
			
			if ( !m_skipMotion ) {
				buildMotionAssociation(nodeTracks, associations, closedTracklets, finalAssociations);
			}
			
			if ( !m_skipFeature ) {
				buildFeatureAssociation(builder, nodeTracks, associations, closedTracklets, finalAssociations);
			}
			
			if ( !m_skipGlobalTracks ) {
				buildGlobalTracks(nodeTracks, associations, finalAssociations);
			}
			
			Topology topology = builder.build();
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("use Kafka servers: {}", m_kafkaParams.getBootstrapServers());
				s_logger.info("use Kafka application: {}", m_kafkaParams.getClientId());
			}
			
			Properties props = m_kafkaParams.toStreamProperties();
			KafkaStreams streams = new KafkaStreams(topology, props);
			Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

			if ( m_cleanUp && m_kafkaParams.getClientId() != null ) {
				try {
					m_kafkaParams.deleteConsumerGroup();
				}
				catch ( Exception e ) {
					Throwable cause = Throwables.unwrapThrowable(e);
					if ( cause instanceof GroupIdNotFoundException ) { }
					else {
						s_logger.error("fails to delete consumer-group: id={}, cause={}",
										m_kafkaParams.getClientId(), cause);
						throw Throwables.toException(cause);
					}
				}
				streams.cleanUp();
			}
			
			streams.start();
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}
	
	private void buildMotionAssociation(KStream<String,NodeTrack> nodeTracks,
										AssociationCollection associations, Set<TrackletId> closedTracklets,
										AssociationCollection finalAssociations) {
		MotionAssociationStreamBuilder motionBuilder
			= new MotionAssociationStreamBuilder(m_configs, associations, closedTracklets, finalAssociations);
		motionBuilder.build(nodeTracks);
	}
	
	private void buildFeatureAssociation(StreamsBuilder builder, KStream<String,NodeTrack> nodeTracks,
											AssociationCollection associations, Set<TrackletId> closedTracklets,
											AssociationCollection finalAssociations) {
		KStream<String,TrackFeature> trackFeatures =
			builder
				.stream(m_configs.getTrackFeaturesTopic(),
						Consumed.with(Serdes.String(), JarveySerdes.TrackFeature())
								.withName("from-track-features")
								.withOffsetResetPolicy(m_kafkaParams.getAutoOffsetReset()));
		
		FeatureAssociationStreamBuilder featureBuilder
				= new FeatureAssociationStreamBuilder(m_configs, associations, closedTracklets, finalAssociations);
		featureBuilder.build(nodeTracks, trackFeatures);
	}
	
	@SuppressWarnings("deprecation")
	private void buildGlobalTracks(KStream<String,NodeTrack> nodeTracks,
									AssociationCollection associations,
									AssociationCollection finalAssociations) {
		OverlapAreaRegistry areaRegistry = m_configs.getOverlapAreaRegistry();
		
		Duration delay = m_configs.getOutputDelay();
		if ( !(delay.isZero() || delay.isNegative()) ) {
			nodeTracks = nodeTracks.flatTransform(() -> new DelayedTransformer<>(delay));
		}
		
		RunningGlobalTrackGenerator gtrackGen = new RunningGlobalTrackGenerator(areaRegistry, associations,
																				finalAssociations);
		nodeTracks
			.flatMap(gtrackGen, Named.as("generate-global-track"))
			.to(m_configs.getGlobalTracksTopic(),
				Produced.with(Serdes.String(), AssocSerdes.GlobalTrack())
						.withName("to-global-tracks-tentative"));
	}
	
	public static final void main(String... args) throws Exception {
		AssociatorMain cmd = new AssociatorMain();
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
