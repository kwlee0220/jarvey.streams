package jarvey.streams.assoc;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.UnitUtils;
import utils.func.FOption;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.streams.assoc.feature.MCMOTNetwork;
import jarvey.streams.assoc.motion.OverlapAreaRegistry;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MCMOTConfig {
	private static final Logger s_logger = LoggerFactory.getLogger(MCMOTConfig.class);
	private static final String DEFAULT_KAFKA_SERVERS = "localhost:9092";
	private static final String DEFAULT_JDBC_URL = "postgresql:localhost:5432:dna:urc2004:dna";
	
	public static final String TOPIC_NODE_TRACKS = "node-tracks";
	public static final String TOPIC_TRACK_FEATURES = "track-features";
	public static final String TOPIC_TRACKLET_ASSOCIATEDS = "tracklet-associateds";
	public static final String TOPIC_GLOBAL_TRACKS = "global-tracks";
	
	private static final Map<String,String> DEFAULT_TOPICS = Maps.newHashMap();
	private static final Map<String,String> DEFAULT_TABLES = Maps.newHashMap();
	static {
		DEFAULT_TOPICS.put(TOPIC_NODE_TRACKS, "node-tracks");
		DEFAULT_TOPICS.put(TOPIC_TRACK_FEATURES, "track-features");
		DEFAULT_TOPICS.put(TOPIC_TRACKLET_ASSOCIATEDS, "tracklet-associateds");
		DEFAULT_TOPICS.put(TOPIC_GLOBAL_TRACKS, "global-tracks");
		
		DEFAULT_TABLES.put("node-tracks", "node_tracks_index");
		DEFAULT_TABLES.put("track-features", "track_features_index");
		DEFAULT_TABLES.put("tracklet-associateds", "tracklet-associateds");
		DEFAULT_TABLES.put("associations", "associations");
	}

	private String m_bootstrapServers = DEFAULT_KAFKA_SERVERS;
	private final Map<String,String> m_topics;

	private JdbcProcessor m_jdbc;
	private final Map<String,String> m_tables = Maps.newHashMap();
	
	private MotionConfigs m_motionConfigs;
	private FeatureConfigs m_featureConfigs;
	private OutputConfigs m_outputConfigs;
	
	public static class MotionConfigs {
		private OverlapAreaRegistry m_areaRegistry;
		private Duration m_assocInterval;
		private Duration m_advanceTime;
		private Duration m_graceTime;
		private double m_maxMatchDistance;

		public OverlapAreaRegistry getOverlapAreaRegistry() {
			return m_areaRegistry;
		}
		
		public double getMaxTrackDistance() {
			return m_maxMatchDistance;
		}
		
		public Duration getMatchingWindowSize() {
			return m_assocInterval;
		}
		
		public Duration getMatchingGraceTime() {
			return m_graceTime;
		}
		
		public Duration getMatchingAdvanceTime() {
			return m_advanceTime;
		}
	}
	
	public static class FeatureConfigs {
		private MCMOTNetwork m_mcmotNetwork;
		private Set<String> m_listeningNodes;
		private Duration m_matchDelay;
		private double m_minBinaryAssociationScore;
		private double m_topPercent;
		
		public MCMOTNetwork getMCMOTNetwork() {
			return m_mcmotNetwork;
		}
		
		public Set<String> getListeningNodes() {
			return m_listeningNodes;
		}
		
		public Duration getMatchDelay() {
			return m_matchDelay;
		}
		
		public double getTopPercent() {
			return m_topPercent;
		}
		
		public double getMinBinaryAssociationScore() {
			return m_minBinaryAssociationScore;
		}
	}
	
	public static class OutputConfigs {
		private Duration m_maxAssociationDelay;
		private int m_cacheSize;
		private Duration m_interval;
		private Duration m_graceTime;
		
		public Duration getMaxAssociationDelay() {
			return m_maxAssociationDelay;
		}
		
		public int getCacheSize() {
			return m_cacheSize;
		}
		
		public Duration getInterval() {
			return m_interval;
		}
		
		public Duration getGraceTime() {
			return m_graceTime;
		}
	}
	
	public MCMOTConfig() {
		m_topics = Maps.newHashMap(DEFAULT_TOPICS);
	}
	
	@SuppressWarnings("unchecked")
	public static MCMOTConfig load(Path configFile) throws IOException {
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("reading a configuration from {}", configFile);
		}
		
		MCMOTConfig configs = new MCMOTConfig();
		Path configDir = configFile.getParent();

		Map<String,Object> empty = Collections.emptyMap();
		Map<String, Object> configDescs = new Yaml().load(new FileReader(configFile.toFile()));

		Map<String,Object> kafka = (Map<String,Object>)configDescs.getOrDefault("kafka", empty);
		configs.m_bootstrapServers = (String)kafka.getOrDefault("bootstrap-servers", DEFAULT_KAFKA_SERVERS);
		
		Map<String,String> topics = (Map<String,String>)kafka.getOrDefault("topics", Maps.newHashMap());
		configs.m_topics.putAll(topics);

		String jdbcUrl = (String)configDescs.getOrDefault("jdbc", DEFAULT_JDBC_URL);
		JdbcParameters jdbcParams = new JdbcParameters();
		jdbcParams.jdbcLoc(jdbcUrl);
		configs.m_jdbc = jdbcParams.createJdbcProcessor();
		
		Map<String,String> tables = (Map<String,String>)configDescs.get("tables");
		configs.m_tables.putAll(tables);
		
		Map<String,Object> configMap;
		configMap = (Map<String,Object>)configDescs.get("motion");
		if ( configMap != null ) {
			configs.m_motionConfigs = parseMotionConfigs(configDir, configMap);
		}
		
		configMap = (Map<String,Object>)configDescs.get("feature");
		if ( configMap != null ) {
			configs.m_featureConfigs = parseFeatureConfigs(configDir, configMap);
		}
		
		Map<String,Object> outConfigMap = (Map<String,Object>)configDescs.get("output");
		if ( outConfigMap == null ) {
			throw new ConfigurationException("Configuration missing: 'output'");
		}
		configs.m_outputConfigs = parseOutputConfigs(outConfigMap);
		
		return configs;
	}
	
	public String getKafkaBootstrapServers() {
		return m_bootstrapServers;
	}
	
	public String getNodeTracksTopic() {
		return m_topics.get(TOPIC_NODE_TRACKS);
	}
	public String getTrackFeaturesTopic() {
		return  m_topics.get(TOPIC_TRACK_FEATURES);
	}
	public String getTrackletAssociatedsTopic() {
		return m_topics.get(TOPIC_TRACKLET_ASSOCIATEDS);
	}
	public String getGlobalTracksTopic() {
		return m_topics.get(TOPIC_GLOBAL_TRACKS);
	}
	
	public FOption<String> getTableName(String key) {
		return FOption.ofNullable(m_tables.get(key));
	}
	
	public MotionConfigs getMotionConfigs() {
		return m_motionConfigs;
	}
	
	public FeatureConfigs getFeatureConfigs() {
		return m_featureConfigs;
	}
	
	public OutputConfigs getOutputConfigs() {
		return m_outputConfigs;
	}
	
	public JdbcProcessor getJdbcProcessor() {
		return m_jdbc;
	}
	
	@SuppressWarnings("unchecked")
	private static MotionConfigs parseMotionConfigs(Path configDir, Map<String,Object> confMap) throws IOException {
		MotionConfigs motionConfigs = new MotionConfigs();
		
		String overlapAreaFilePath = (String)confMap.getOrDefault("overlap-area", "overlap_areas.yaml");
		Path ovAreaConfigFile = Paths.get(overlapAreaFilePath);
		if ( !ovAreaConfigFile.isAbsolute() ) {
			ovAreaConfigFile = configDir.resolve(overlapAreaFilePath);
		}
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("reading motion configuration: {}", ovAreaConfigFile);
		}
		motionConfigs.m_areaRegistry = OverlapAreaRegistry.load(ovAreaConfigFile);

		String str;
		Map<String,String> matchConfigs = (Map<String,String>)confMap.getOrDefault("match",
																					Collections.emptyMap());
		str = matchConfigs.getOrDefault("window-size", "1s");
		motionConfigs.m_assocInterval = UnitUtils.parseDuration(str);
		str = matchConfigs.getOrDefault("advance-time", null);
		motionConfigs.m_advanceTime = (str != null) ? UnitUtils.parseDuration(str) : null;
		str = matchConfigs.getOrDefault("grace-time", null);
		motionConfigs.m_graceTime = (str != null) ? UnitUtils.parseDuration(str) : null;
		str = matchConfigs.getOrDefault("max-distance", "5m");
		motionConfigs.m_maxMatchDistance = UnitUtils.parseLengthInMeter(str);
		
		return motionConfigs;
	}
	
	@SuppressWarnings("unchecked")
	private static FeatureConfigs parseFeatureConfigs(Path configDir, Map<String,Object> confMap) throws IOException {
		FeatureConfigs featureConfigs = new FeatureConfigs();
		
		String networkFilePath = (String)confMap.getOrDefault("camera-network", "mcmot_network.yaml");
		Path networkConfigFile =Paths.get(networkFilePath);
		if ( !networkConfigFile.isAbsolute() ) {
			networkConfigFile = configDir.resolve(networkFilePath);
		}
		featureConfigs.m_mcmotNetwork = MCMOTNetwork.load(networkConfigFile);
		
		List<String> nodeList = (List<String>)confMap.get("listening-nodes");
		if ( nodeList != null ) {
			featureConfigs.m_listeningNodes = Sets.newHashSet(nodeList);
			Set<String> listeningNodesDiff = Sets.difference(featureConfigs.m_listeningNodes,
															featureConfigs.m_mcmotNetwork.getListeningNodeAll());
			if ( !listeningNodesDiff.isEmpty() ) {
				s_logger.warn("Some listening nodes are not specified in the network configuration: nodes={}",
								listeningNodesDiff);
				featureConfigs.m_listeningNodes = Sets.intersection(featureConfigs.m_listeningNodes,
															featureConfigs.m_mcmotNetwork.getListeningNodeAll());
			}
		}
		else {
			featureConfigs.m_listeningNodes = featureConfigs.m_mcmotNetwork.getListeningNodeAll();
		}

		Map<String,Object> matchConfigs = (Map<String,Object>)confMap.getOrDefault("match",
																					Collections.emptyMap());
		String delayStr = (String)matchConfigs.getOrDefault("delay", "0s");
		featureConfigs.m_matchDelay = UnitUtils.parseDuration(delayStr);
		featureConfigs.m_minBinaryAssociationScore = (double)matchConfigs.getOrDefault("min-score", 0.3);
		featureConfigs.m_topPercent = (double)matchConfigs.getOrDefault("top-percent", 0.2);
		
		return featureConfigs;
	}
	
	private static OutputConfigs parseOutputConfigs(Map<String,Object> confMap) {
		OutputConfigs outputConfigs = new OutputConfigs();

		outputConfigs.m_maxAssociationDelay = UnitUtils.parseDuration(
												(String)confMap.getOrDefault("max-association-delay", "3m"));
		outputConfigs.m_cacheSize = (int)confMap.getOrDefault("cache-size", 128);
		outputConfigs.m_interval = UnitUtils.parseDuration((String)confMap.getOrDefault("interval", "100ms"));
		outputConfigs.m_graceTime = UnitUtils.parseDuration((String)confMap.getOrDefault("grace-time", "100ms"));
		
		return outputConfigs;
	}
}
	
