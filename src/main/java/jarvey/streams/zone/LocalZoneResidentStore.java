package jarvey.streams.zone;

import java.util.List;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import utils.func.KeyValue;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LocalZoneResidentStore implements ZoneResidentsStore {
	private final ReadOnlyKeyValueStore<GlobalZoneId,Residents> m_store;
	
	LocalZoneResidentStore(KafkaStreams streams, String storeName) {
		m_store = streams.store(StoreQueryParameters.fromNameAndType(storeName,
								QueryableStoreTypes.keyValueStore()));
	}
	
	@Override
	public Residents getResidentsOfZone(GlobalZoneId zoneId) {
		return m_store.get(zoneId);
	}

	@Override
	public List<KeyValue<GlobalZoneId,Residents>> getResidentsOfNode(String nodeId) {
		return FStream.from(m_store.all())
					.filter(kv -> kv.key.getNodeId().equals(nodeId))
					.map(kv -> KeyValue.of(kv.key, kv.value))
					.toList();
	}

	@Override
	public List<KeyValue<GlobalZoneId,Residents>> getResidentsAll() {
		return FStream.from(m_store.all())
					.map(kv -> KeyValue.of(kv.key, kv.value))
					.toList();
	}
}
