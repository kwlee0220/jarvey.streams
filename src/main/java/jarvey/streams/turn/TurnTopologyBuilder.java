package jarvey.streams.turn;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.model.GUID;
import jarvey.streams.serialization.json.GsonUtils;
import jarvey.streams.zone.ZoneLineRelationEvent;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TurnTopologyBuilder {
	private static final String STORE_ZONE_SEQUENCES = "zone-sequences";
	private static final Serde<String> KEY_SERDE = Serdes.String();
	
	private String m_topicLocationEvents = "location-events";
	private String m_topicTurnEvents = null;
	
	private TurnTopologyBuilder() {
	}
	
	public static TurnTopologyBuilder create() {
		return new TurnTopologyBuilder();
	}
	
	public TurnTopologyBuilder setLocationEventsTopic(String topic) {
		Objects.requireNonNull(topic, "LocationEventsTopic is null");
		
		m_topicLocationEvents = topic;
		return this;
	}
	
	public TurnTopologyBuilder setTurnEventsTopic(String topic) {
		m_topicTurnEvents = topic;
		return this;
	}
	
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	private static <K,V> Consumed<K,V> Consumed(Serde<K> keySerde, Class<V> valueCls) {
		return Consumed.with(keySerde, GsonUtils.getSerde(valueCls))
						.withTimestampExtractor(TS_EXTRACTOR);
	}
	
	public Topology build() {
		Objects.requireNonNull(m_topicLocationEvents, "LocationEventsTopic is null");
		
		StreamsBuilder builder = new StreamsBuilder();
		
		builder.addStateStore(Stores.keyValueStoreBuilder(
											Stores.persistentKeyValueStore(STORE_ZONE_SEQUENCES),
											GUID.getSerde(), GsonUtils.getSerde(ZoneSequence.class)));
		
		KStream<String,ObjectTurn> turns
			= builder.stream(m_topicLocationEvents, Consumed(KEY_SERDE, ZoneLineRelationEvent.class))
					.flatTransformValues(() -> new ZoneSequenceCollector(STORE_ZONE_SEQUENCES), STORE_ZONE_SEQUENCES)
					.flatMapValues(new TurnDetector());
		turns.print(Printed.<String, ObjectTurn>toSysOut().withLabel("turns"));
		if ( m_topicTurnEvents != null ) {
			turns.to(m_topicTurnEvents, Produced.with(KEY_SERDE, GsonUtils.getSerde(ObjectTurn.class)));
		}
		
		return builder.build();
	}
}
