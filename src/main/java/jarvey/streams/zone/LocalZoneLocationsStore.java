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
class LocalZoneLocationsStore implements ZoneLocationsStore {
	private final ReadOnlyKeyValueStore<String,TrackZoneLocations> m_store;
	
	LocalZoneLocationsStore(KafkaStreams streams, String storeName) {
		m_store = streams.store(StoreQueryParameters.fromNameAndType(storeName,
								QueryableStoreTypes.keyValueStore()));
	}
	
	@Override
	public TrackZoneLocations getZoneLocations(String trackId) {
		return m_store.get(trackId);
	}

	@Override
	public List<KeyValue<String,TrackZoneLocations>> getZoneLocationsAll() {
		return FStream.from(m_store.all())
					.map(kv -> KeyValue.of(kv.key, kv.value))
					.toList();
	}
}
