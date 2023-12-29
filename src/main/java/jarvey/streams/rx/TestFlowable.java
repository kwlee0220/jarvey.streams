package jarvey.streams.rx;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;

import jarvey.streams.node.NodeTrack;
import jarvey.streams.serialization.json.GsonUtils;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TestFlowable {
	public static final void main(String... args) throws Exception {
		Properties props = new Properties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "xxx3");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());

		long started = System.currentTimeMillis();
		KafkaPoller poller = KafkaPoller.stopOnTimeout(Duration.ofSeconds(3))
											.initialTimeout(Duration.ofSeconds(5));
		KafkaUtils.<String,String>observeKafkaTopics(props, "node-tracks", poller)
			.map(ConsumerRecord::value)
			.map(r -> GsonUtils.parseJson(r, NodeTrack.class))
			.groupBy(t -> t.getNodeId())
			.flatMapSingle(g -> g.count().map(c -> g.getKey() + ": " + c))
//			.flatMapSingle(g -> g.observeOn(Schedulers.io()).count().map(c -> g.getKey() + ": " + c))
			.blockingSubscribe(System.out::println);
		;
		long elapsed = System.currentTimeMillis() - started;
		System.out.printf("elapsed=%.3f%n", elapsed / 1000f);
	}
}