/**
 * 
 */
package jarvey.streams.zone;

import java.util.List;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import utils.func.KeyValue;
import utils.stream.FStream;

import jarvey.streams.model.GUID;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class LocalZoneLocationsStore implements ZoneLocationsStore {
	private final ReadOnlyKeyValueStore<GUID,ZoneLocations> m_store;
	
	LocalZoneLocationsStore(KafkaStreams streams, String storeName) {
		m_store = streams.store(StoreQueryParameters.fromNameAndType(storeName,
								QueryableStoreTypes.keyValueStore()));
	}
	
	@Override
	public ZoneLocations getZoneLocationsOfObject(GUID zoneId) {
		return m_store.get(zoneId);
	}

	@Override
	public List<KeyValue<GUID,ZoneLocations>> getZoneLocationsOfNode(String nodeId) {
		return FStream.from(m_store.all())
					.filter(kv -> kv.key.getNodeId().equals(nodeId))
					.map(kv -> KeyValue.of(kv.key, kv.value))
					.toList();
	}

	@Override
	public List<KeyValue<GUID,ZoneLocations>> getZoneLocationsAll() {
		return FStream.from(m_store.all())
					.map(kv -> KeyValue.of(kv.key, kv.value))
					.toList();
	}
}
