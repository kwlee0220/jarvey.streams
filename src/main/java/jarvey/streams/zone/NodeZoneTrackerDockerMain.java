package jarvey.streams.zone;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;

import utils.NetUtils;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NodeZoneTrackerDockerMain {
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();

		String appId = envs.getOrDefault("KAFKA_APPLICATION_ID_CONFIG", "node-track");
		String kafkaServers = envs.getOrDefault("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");
		String topicNodeTracks = envs.getOrDefault("DNA_TOPIC_TRACKS", "node-tracks");
		String topicZoneLineRelations = envs.get("DNA_TOPIC_ZONE_LINE_RELATIONS");
		String topicLocationEvents = envs.getOrDefault("DNA_TOPIC_LOCATION_EVENTS", "location-events");
		String topicZoneLocations = envs.get("DNA_TOPIC_ZONE_LOCATIONS");
		String topicZoneResidents = envs.get("DNA_TOPIC_ZONE_RESIDENTS");
		
		String jdbcUrl = envs.getOrDefault("DNA_ZONE_DB_JDBC_URL", "jdbc:postgresql://localhost:5432/dna");
		String user = envs.getOrDefault("DNA_ZONE_DB_USER", "dna");
		String password = envs.getOrDefault("DNA_ZONE_DB_PASSWORD", "urc2004");
		JdbcProcessor jdbc = JdbcProcessor.create(jdbcUrl, user, password);
		
		Topology topology = TrackTopologyBuilder.create()
												.setNodeTracksTopic(topicNodeTracks)
												.setZoneLineRelationsTopic(topicZoneLineRelations)
												.setLocationEventsTopic(topicLocationEvents)
												.setZoneLocationsTopic(topicZoneLocations)
												.setZoneResidentsTopic(topicZoneResidents)
												.setJdbcProcessor(jdbc)
												.build();
		
		String restHost = envs.getOrDefault("DNA_APP_REST_HOST", NetUtils.getLocalHostAddress());
		int restPort = Integer.parseInt(envs.getOrDefault("DNA_APP_REST_PORT", "15685"));
		String appServerUrl = String.format("%s:%d", restHost, restPort);
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, appServerUrl);
//		config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		
		KafkaStreams streams = new KafkaStreams(topology, config);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
		streams.start();
		
		// start the REST service
		HostInfo hostInfo = new HostInfo(restHost, restPort);
		RESTfulObjectTrackingService service = new RESTfulObjectTrackingService(hostInfo, streams);
		service.start();
	}
}
