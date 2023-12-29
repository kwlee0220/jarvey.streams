package jarvey.streams.rx;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import jarvey.streams.model.TrackFeatureSerde;
import jarvey.streams.node.TrackFeature;

import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TestFStream {
	public static class KafkaObservableOnSubscribe implements ObservableOnSubscribe<ConsumerRecord<String,String>> {
		private final KafkaConsumer<String,String> m_consumer;
		private final List<String> m_topics;
		
		KafkaObservableOnSubscribe(KafkaConsumer<String,String> consumer, List<String> topics) {
			m_consumer = consumer;
			m_topics = topics;
		}

		@Override
		public void subscribe(ObservableEmitter<ConsumerRecord<String,String>> emitter) throws Throwable {
			m_consumer.subscribe(m_topics);

			// 첫번째 poll할 때는 'initial poll timeout'을 사용한다.
			Duration pollTimeout = Duration.ofSeconds(10);
			
			while ( true ) {
				ConsumerRecords<String,String> records = m_consumer.poll(pollTimeout);
				
				// 두번재 이후 poll부터 'poll timeout'를 사용하도록 설정한다.
				pollTimeout = Duration.ofSeconds(3);
				
				if ( records.count() > 0 ) {
					for ( TopicPartition tpart: records.partitions() ) {
						for ( ConsumerRecord<String,String> rec: records.records(tpart) ) {
							emitter.onNext(rec);
						}
					}
				}
				else {
					emitter.onComplete();
					return;
				}
			}
		}
	};
	
	public static final void main(String... args) throws Exception {
		Properties props = new Properties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "xxx3");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.ByteArray().deserializer().getClass().getName());

		Deserializer<TrackFeature> deser = TrackFeatureSerde.getInstance().deserializer();
		KafkaPoller poller = KafkaPoller.stopOnTimeout(Duration.ofSeconds(3))
											.initialTimeout(Duration.ofSeconds(5));
		KafkaUtils.<String,byte[]>streamKafkaTopic(props, "track-features", poller)
				.map(kv -> deser.deserialize(kv.topic(), kv.value()))
				.take(100)
				.forEach(System.out::println);
//			.filter(r -> r.key().equals("etri:04"))
//			.map(ConsumerRecord::key)
//			.forEach(System.out::println);
//			.map(r -> GsonUtils.parseJson(r.value(), NodeTrack.class))
//			.groupBy(t -> t.getTrackId())
//			.flatMapSingle(g -> g.filter(t -> t.isDeleted()).count())
//			.blockingSubscribe(System.out::println);
		;
	}
}