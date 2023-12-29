package jarvey.streams.processor;

import org.apache.kafka.common.TopicPartition;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface KafkaProcessorAllocator<K,V> {
	/**
	 * 주어진 topic partition을 처리할 {@link KafkaTopicPartitionProcessor} 객체를 할당하여 반환한다.
	 * 
	 * 생성된 processor는 전체 프로그램이 종료될 {@link KafkaTopicPartitionProcessor#close}를
	 * 통해 release된다.
	 * 생성된 processor는 다른 partition을 위해서도 공통으로 활용될 수 있다.
	 * 
	 * @param tpart	 topic {@link TopicPartition} 정보.
	 * @return	{@link KafkaTopicPartitionProcessor} 객체.
	 * @throws Exception	processor 객체 생성 중 오류가 발생된 경우.
	 */
	public KafkaTopicPartitionProcessor<K,V> allocateProcessor(TopicPartition tpart) throws Exception;
}
