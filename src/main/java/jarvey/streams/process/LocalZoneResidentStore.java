/**
 * 
 */
package jarvey.streams.process;

import java.util.List;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.streams.model.GlobalZoneId;
import jarvey.streams.model.Residents;
import utils.func.KeyValue;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LocalZoneResidentStore {
	private final static Logger s_logger = LoggerFactory.getLogger(LocalZoneResidentStore.class);

	private final ReadOnlyKeyValueStore<GlobalZoneId,Residents> m_store;
	
	LocalZoneResidentStore(KafkaStreams streams, String storeName) {
		m_store = streams.store(StoreQueryParameters.fromNameAndType(storeName,
								QueryableStoreTypes.keyValueStore()));
	}
	
	public Residents getResidentsInZone(GlobalZoneId zoneId) {
		return m_store.get(zoneId);
	}
	
	public List<KeyValue<GlobalZoneId,Residents>> getResidentsInNode(String nodeId) {
		return FStream.from(m_store.all())
					.filter(kv -> kv.key.getNodeId().equals(nodeId))
					.map(kv -> KeyValue.of(kv.key, kv.value))
					.toList();
	}
	
	public List<KeyValue<GlobalZoneId,Residents>> getResidentsAll() {
		return FStream.from(m_store.all())
					.map(kv -> KeyValue.of(kv.key, kv.value))
					.toList();
	}
}
