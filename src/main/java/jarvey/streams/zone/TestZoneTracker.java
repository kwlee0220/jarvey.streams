package jarvey.streams.zone;

import java.io.FileReader;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import utils.UnitUtils;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;
import utils.stream.KVFStream;

import io.confluent.common.utils.TestUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestZoneTracker {
	private static final Logger s_logger = LoggerFactory.getLogger(TestZoneTracker.class.getPackage().getName());
	
	private static final Map<String,Range<Double>> VALID_DISTANCE_THRESHOLD = Maps.newHashMap();
	static {
		VALID_DISTANCE_THRESHOLD.put("etri:04", Range.closed(37d, 50d));
		VALID_DISTANCE_THRESHOLD.put("etri:05", Range.closed(50d, 60d));
		VALID_DISTANCE_THRESHOLD.put("etri:06", Range.closed(36d, 40d));
		VALID_DISTANCE_THRESHOLD.put("etri:07", Range.closed(40d, 47d));
	}
	
	private static Map<String,Polygon> loadZones() throws Exception {
		Map<String,Object> zones = new Yaml().load(new FileReader("zones.yml"));
		Map<String,Object> map = (Map<String,Object>)zones.get("zones");
		return KVFStream.from(map)
						.mapValue(v -> loadPolygon((List)v))
						.toMap();
	}
	
	private static double asDouble(Object v) {
		if ( v instanceof Integer ) {
			return (double)(int)v;
		}
		else if ( v instanceof Double ) {
			return (double)v;
		}
		else {
			return Double.parseDouble("" + v);
		}
	}
	
	private static Coordinate toCoordinate(List<?> values) {
		return new Coordinate(asDouble(values.get(0)), asDouble(values.get(1)));
	}
	
	private static Polygon loadPolygon(List<Object> obj) {
		Coordinate[] coords = FStream.from(obj)
										.cast(List.class)
										.map(v -> toCoordinate(v))
										.toArray(Coordinate.class);
		return GeoUtils.toPolygon(coords);
	}
	
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();
		
		List<Zone> zones = KVFStream.from(loadZones())
									.map((zid, region) -> new Zone(zid, region))
									.toList();
		
		String topic = "node-tracks";
		Duration pollTimeout = Duration.ofMillis(1000);
		
		Properties props = new Properties();
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_global_locator");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int)UnitUtils.parseDurationMillis("10s"));
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int)UnitUtils.parseDurationMillis("30s"));

		final KafkaConsumer<String, Bytes> consumer = new KafkaConsumer<>(props);
		
		Thread mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					consumer.wakeup();
					mainThread.join();
				}
				catch ( InterruptedException e ) {
					s_logger.error("interrupted", e);
				}
			}
		});
		
		Topology topology = ZoneTrackTopologyBuilder.create()
												.setZones(zones)
												.setNodeTracksTopic("node-tracks")
												.setZoneTracksTopic("zone-tracks")
												.setZoneResidentsTopic("zone-residents")
												.build();
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "track-zone");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class);
		config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
		
		KafkaStreams streams = new KafkaStreams(topology, config);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
		streams.start();
	}
}
