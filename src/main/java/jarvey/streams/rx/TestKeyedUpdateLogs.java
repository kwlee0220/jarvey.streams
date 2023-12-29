package jarvey.streams.rx;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import utils.jdbc.JdbcProcessor;

import jarvey.streams.model.TrackFeatureSerde;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.updatelog.KeyedUpdateIndex;
import jarvey.streams.updatelog.KeyedUpdateLogs;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TestKeyedUpdateLogs {
	public static final void main(String... args) throws Exception {
		JdbcProcessor proc = JdbcProcessor.parseString("postgresql:localhost:5432:dna:urc2004:dna");
		
		Properties props = new Properties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "xxx3");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.ByteArray().deserializer().getClass().getName());

		Deserializer<TrackFeature> featureDeser = TrackFeatureSerde.s_deserializer;
		KeyedUpdateLogs<TrackFeature> store = new KeyedUpdateLogs<>(proc, "track_features_index", props,
																	"track-features", featureDeser);
		
//		store.fstream()
//			.take(5)
//			.forEach(System.out::println);
//		
		KeyedUpdateIndex idx = store.fstream().drop(5).findFirst().get();
		System.out.println(idx);
		
		store.streamKeyedUpdatesOfKey(idx.getKey())
			.forEach(System.out::println);
	}
}