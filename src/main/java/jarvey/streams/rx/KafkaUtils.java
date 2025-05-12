package jarvey.streams.rx;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import utils.Tuple;
import utils.func.Funcs;
import utils.rx.Flowables;
import utils.stream.FStream;

import io.reactivex.rxjava3.core.Flowable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class KafkaUtils {
	public static <K,V> FStream<ConsumerRecord<K,V>>
	streamKafkaTopics(KafkaConsumer<K,V> consumer, Collection<String> topics, KafkaPoller poller) {
		consumer.subscribe(topics);
		return poller.start(consumer);
	}
	public static <K,V> FStream<ConsumerRecord<K,V>>
	streamKafkaTopics(Properties consumerProps, Collection<String> topics, KafkaPoller poller) {
		KafkaConsumer<K,V> consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(topics);
		
		return poller.start(consumer, KafkaConsumer::close);
	}
	public static <K,V> FStream<ConsumerRecord<K,V>>
	streamKafkaTopic(Properties consumerProps, String topic, KafkaPoller poller) {
		KafkaConsumer<K,V> consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(Collections.singletonList(topic));
		
		return poller.start(consumer, KafkaConsumer::close);
	}
	
	public static <K,V> FStream<ConsumerRecord<K,V>>
	streamKafkaTopicPartions(KafkaConsumer<K,V> consumer, List<Tuple<TopicPartition,Long>> partInfos,
							KafkaPoller poller) {
		consumer.assign(Funcs.map(partInfos, Tuple::_1));
		for ( Tuple<TopicPartition,Long> tpart: partInfos ) {
			consumer.seek(tpart._1, tpart._2);
		}
		
		return poller.start(consumer);
	}
	public static <K,V> FStream<ConsumerRecord<K,V>>
	streamKafkaTopicPartions(Properties consumerProps, List<Tuple<TopicPartition,Long>> partInfos,
							 KafkaPoller poller) {
		KafkaConsumer<K,V> consumer = new KafkaConsumer<>(consumerProps);
		consumer.assign(Funcs.map(partInfos, Tuple::_1));
		for ( Tuple<TopicPartition,Long> tpart: partInfos ) {
			consumer.seek(tpart._1, tpart._2);
		}
		
		return poller.start(consumer, KafkaConsumer::close);
	}
	public static <K,V> FStream<ConsumerRecord<K,V>>
	streamKafkaTopicPartions(Properties consumerProps, TopicPartition tpart, long startOffset,
							 KafkaPoller poller) {
		KafkaConsumer<K,V> consumer = new KafkaConsumer<>(consumerProps);
		consumer.assign(Collections.singletonList(tpart));
		consumer.seek(tpart, startOffset);
		
		return poller.start(consumer, KafkaConsumer::close);
	}
	
	public static <K,V> Flowable<ConsumerRecord<K,V>>
	observeKafkaTopics(KafkaConsumer<K,V> consumer, Collection<String> topics, KafkaPoller poller) {
		return Flowables.from(streamKafkaTopics(consumer, topics, poller));
	}
	public static <K,V> Flowable<ConsumerRecord<K,V>>
	observeKafkaTopics(KafkaConsumer<K,V> consumer, String topic, KafkaPoller poller) {
		return Flowables.from(streamKafkaTopics(consumer, Arrays.asList(topic), poller));
	}
	public static <K,V> Flowable<ConsumerRecord<K,V>>
	observeKafkaTopics(Properties consumerProps, Collection<String> topics, KafkaPoller poller) {
		return Flowables.from(streamKafkaTopics(consumerProps, topics, poller));
	}
	public static <K,V> Flowable<ConsumerRecord<K,V>>
	observeKafkaTopics(Properties consumerProps, String topic, KafkaPoller poller) {
		return Flowables.from(streamKafkaTopics(consumerProps, Arrays.asList(topic), poller));
	}
	
	public static <K,V> Flowable<ConsumerRecord<K,V>>
	observeKafkaTopicPartions(KafkaConsumer<K,V> consumer, List<Tuple<TopicPartition,Long>> partInfos,
							KafkaPoller poller) {
		return Flowables.from(streamKafkaTopicPartions(consumer, partInfos, poller));
	}
	public static <K,V> Flowable<ConsumerRecord<K,V>>
	observeKafkaTopicPartions(Properties consumerProps, List<Tuple<TopicPartition,Long>> partInfos,
							KafkaPoller poller) {
		return Flowables.from(streamKafkaTopicPartions(consumerProps, partInfos, poller));
	}
}