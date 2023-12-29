package jarvey.streams.assoc.tool;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;

import utils.LoggerNameBuilder;
import utils.Throwables;
import utils.func.FOption;

import jarvey.streams.KafkaOptions;
import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.assoc.AssociationCollection;
import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.assoc.feature.FeatureAssociationStreamBuilder;
import jarvey.streams.assoc.motion.MotionAssociationStreamBuilder;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="associations",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="NodeTrack association")
final class AssociateTrackletMain extends JarveyStreamsCommand {
	private static final Logger s_logger = LoggerNameBuilder.from(AssociateTrackletMain.class)
																.dropSuffix(2)
																.append("main")
																.getLogger();
	
	private static final String DEFAULT_APP_ID = "tracklet-associator";
	private static final String KEY_SERDE_NAME = Serdes.String().getClass().getName();
	private static final String VALUE_SERDE_NAME = Serdes.ByteArray().getClass().getName();
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	
	@Option(names={"--skip_motion"}, description="skip motion-based association")
	private boolean m_skipMotion = false;
	
	@Option(names={"--skip_feature"}, description="skip feature-based association")
	private boolean m_skipFeature = false;
	
	@Option(names={"--cleanup"}, description="clean-up Kafka application.")
	private boolean m_cleanUp = false;
	
	public AssociateTrackletMain() {
		setLogger(s_logger);
	}

	private FOption<String> m_appId = FOption.empty();
	private String getApplicationId() {
		String grpId = m_appId.getOrElse(DEFAULT_APP_ID);
		if ( grpId.equalsIgnoreCase("random") ) {
			return UUID.randomUUID().toString().replace("-","").substring(0,12);
		}
		else {
			return grpId;
		}
	}
	@Option(names={"--app_id"}, paramLabel="id", description={"application id"})
	public void setApplicationId(String appId) {
		m_appId = FOption.ofNullable(appId);
	}
	
	private MCMOTConfig m_configs;
	@Mixin private KafkaOptions m_kafkaOptions;
	
	@Override
	public void run(MCMOTConfig configs) throws Exception {
		m_configs = configs;
		
		Properties props = m_kafkaOptions.toStreamProperties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KEY_SERDE_NAME);
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, VALUE_SERDE_NAME);
		
		AssociationCollection associations = new AssociationCollection("associations");
		AssociationCollection finalAssociations = new AssociationCollection("final-associations");
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String,NodeTrack> nodeTracks
			= builder.stream(m_configs.getNodeTracksTopic(),
							Consumed.with(Serdes.String(), JarveySerdes.NodeTrack())
									.withName("from-node-tracks")
									.withTimestampExtractor(TS_EXTRACTOR)
									.withOffsetResetPolicy(m_kafkaOptions.getKafkaOfffset()));
		
		// 설정 상황에 따라 motion-based association stream과 feature-based association stream을
		// 생성한다. 각각의 stream의 최종 결과는 각 tracklet이 어느 lead tracklet에 association되었는가에
		// 대한 event가 생성되고, 그 stream들의 merge된 stream이 최종 결과가 된다.ㅏ
		KStream<String,String> trackletAssociateds = null;
		if ( !m_skipMotion && configs.getMotionConfigs() != null ) {
			trackletAssociateds = buildMotionAssociation(nodeTracks, associations, finalAssociations);
			if ( getLogger().isInfoEnabled() ) {
				getLogger().info("build motion-based association");
			}
		}
		else {
			if ( getLogger().isInfoEnabled() ) {
				getLogger().info("skip motion-based association");
			}
		}
		if ( !m_skipFeature && configs.getFeatureConfigs() != null ) {
			KStream<String,String> associateds
				= buildFeatureAssociation(builder, nodeTracks, associations, finalAssociations);
			if ( trackletAssociateds != null ) {
				trackletAssociateds = trackletAssociateds.merge(associateds, Named.as("association_mergeds"));
			}
			else {
				trackletAssociateds = associateds;
			}
			if ( getLogger().isInfoEnabled() ) {
				getLogger().info("build feature-based association");
			}
		}
		else {
			if ( getLogger().isInfoEnabled() ) {
				getLogger().info("skip feature-based association");
			}
		}
		if ( trackletAssociateds != null ) {
			trackletAssociateds
//				.peek((k,v) -> System.err.printf("publish assigned tracklet: [%s]: %s%n", v, k))
				.to("tracklet-associateds",
					Produced.with(Serdes.String(), Serdes.String())
							.withName("tracklet-associateds"));
		}
		
		Topology topology = builder.build();
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("use Kafka servers: {}, {}", m_kafkaOptions.getBootstrapServersOrDefault(),
														m_kafkaOptions.getKafkaOfffset());
			s_logger.info("use Kafka application: {}", getApplicationId());
		}
		
		KafkaStreams streams = new KafkaStreams(topology, props);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		if ( m_cleanUp && !getApplicationId().equalsIgnoreCase("random") ) {
			try {
				m_kafkaOptions.deleteConsumerGroup(getApplicationId());
			}
			catch ( Exception e ) {
				Throwable cause = Throwables.unwrapThrowable(e);
				if ( cause instanceof GroupIdNotFoundException ) { }
				else {
					s_logger.error("fails to delete consumer-group: id={}, cause={}",
									getApplicationId(), cause);
					throw Throwables.toException(cause);
				}
			}
			streams.cleanUp();
		}
		
		streams.start();
	}
	
	private KStream<String,String>
	buildMotionAssociation(KStream<String,NodeTrack> nodeTracks,
							AssociationCollection associations, AssociationCollection finalAssociations) {
		MotionAssociationStreamBuilder motionBuilder
			= new MotionAssociationStreamBuilder(m_configs, associations, finalAssociations);
		return motionBuilder.build(nodeTracks);
	}
	
	private KStream<String,String>
	buildFeatureAssociation(StreamsBuilder builder, KStream<String,NodeTrack> nodeTracks,
							AssociationCollection associations, AssociationCollection finalAssociations) {
		KStream<String,TrackFeature> trackFeatures =
			builder
				.stream(m_configs.getTrackFeaturesTopic(),
						Consumed.with(Serdes.String(), JarveySerdes.TrackFeature())
								.withName("from-track-features")
								.withOffsetResetPolicy(m_kafkaOptions.getKafkaOfffset()));
		
		FeatureAssociationStreamBuilder featureBuilder = new FeatureAssociationStreamBuilder(m_configs);
		return featureBuilder.build(nodeTracks, trackFeatures);
	}
	
	public static final void main(String... args) throws Exception {
		AssociateTrackletMain cmd = new AssociateTrackletMain();
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
