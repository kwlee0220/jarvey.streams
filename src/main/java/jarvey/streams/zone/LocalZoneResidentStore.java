package jarvey.streams.zone;

import java.util.List;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import utils.func.KeyValue;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LocalZoneResidentStore implements ZoneResidentsStore {
	private final ReadOnlyKeyValueStore<String,Residents> m_store;
	
	LocalZoneResidentStore(KafkaStreams streams, String storeName) {
		m_store = streams.store(StoreQueryParameters.fromNameAndType(storeName,
								QueryableStoreTypes.keyValueStore()));
	}
	
	@Override
	public Residents getResidentsOfZone(String zoneId) {
		return m_store.get(zoneId);
	}

	@Override
	public List<KeyValue<String,Residents>> getResidentsAll() {
		try ( KeyValueIterator<String, Residents> iter = m_store.all() ) {
			return FStream.from(iter)
							.map(kv -> KeyValue.of(kv.key, kv.value))
							.toList();
		}
	}
}
