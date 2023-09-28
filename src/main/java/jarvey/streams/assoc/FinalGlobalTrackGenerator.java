package jarvey.streams.assoc;

import java.sql.SQLException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import jarvey.streams.assoc.motion.OverlapAreaRegistry;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;
import jarvey.streams.serialization.json.GsonUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class FinalGlobalTrackGenerator extends AbstractGlobalTrackGenerator
										implements KafkaConsumerRecordProcessor<String,byte[]> {
	private final AssociationStore m_assocStore;
	private final KafkaProducer<String,byte[]> m_producer;
	private final String m_outputTopic;
	
	private final Deserializer<NodeTrack> m_deser;
	private final Serializer<GlobalTrack> m_ser;
	private final AssociationCache m_cache = new AssociationCache(64);
	
	public FinalGlobalTrackGenerator(OverlapAreaRegistry areaRegistry, 
										AssociationStore assocStore,
										KafkaProducer<String,byte[]> producer, String outputTopic) {
		super(areaRegistry);
		
		m_assocStore = assocStore;
		m_producer = producer;
		m_outputTopic = outputTopic;
		
		m_deser = JarveySerdes.NodeTrack().deserializer();
		m_ser = GsonUtils.getSerde(GlobalTrack.class).serializer();
	}

	@Override
	public void close() throws Exception {
		closeGenerator().forEach(this::publish);
	}

	@Override
	public ProcessResult process(ConsumerRecord<String, byte[]> record) {
		NodeTrack track = m_deser.deserialize(record.topic(), record.value());
		
		generate(track).forEach(this::publish);
		
		return new ProcessResult(record.offset() + 1);
	}

	@Override
	public void timeElapsed(long expectedTs) {
		handleTimeElapsed(expectedTs).forEach(this::publish);
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		return m_deser.deserialize(record.topic(), record.value()).getTimestamp();
	}
	
	@Override
	protected Association findAssociation(LocalTrack ltrack) {
		return m_cache.get(ltrack.getTrackletId())
						.getOrElse(() -> {
							try {
								Association assoc = m_assocStore.getAssociation(ltrack.getTrackletId());
								m_cache.put(ltrack.getTrackletId(), assoc);
								return assoc;
							}
							catch ( SQLException e ) {
								return null;
							}
						});
	}
	
	private void publish(GlobalTrack gtrack) {
		byte[] bytes = m_ser.serialize(m_outputTopic, gtrack);
		m_producer.send(new ProducerRecord<>(m_outputTopic, gtrack.getKey(), bytes));
	}
}
