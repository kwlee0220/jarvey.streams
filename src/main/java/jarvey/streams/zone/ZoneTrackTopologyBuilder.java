package jarvey.streams.zone;

import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import com.google.common.collect.Lists;

import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class ZoneTrackTopologyBuilder {
	private static final Serde<String> KEY_SERDE = Serdes.String();
	private static final String STORE_ZONE_LOCATIONS = "zone-locations";
	private static final String STORE_ZONE_RESIDENTS = "zone-residents";
	
	private List<Zone> m_zones = Lists.newArrayList();
	private String m_topicNodeTracks = "node-tracks";
	private String m_topicZoneTracks = "zone_tracks";
	private String m_topicZoneResidents = null;
	
	private ZoneTrackTopologyBuilder() {
	}
	
	static ZoneTrackTopologyBuilder create() {
		return new ZoneTrackTopologyBuilder();
	}
	
	public ZoneTrackTopologyBuilder setZones(List<Zone> zones) {
		Objects.requireNonNull(zones, "zones is null");
		
		m_zones = zones;
		return this;
	}
	
	public ZoneTrackTopologyBuilder setNodeTracksTopic(String topic) {
		Objects.requireNonNull(topic, "NodeTracksTopic is null");
		
		m_topicNodeTracks = topic;
		return this;
	}
	
	public ZoneTrackTopologyBuilder setZoneTracksTopic(String topic) {
		m_topicZoneTracks = topic;
		return this;
	}
	
	public ZoneTrackTopologyBuilder setZoneResidentsTopic(String topic) {
		m_topicZoneResidents = topic;
		return this;
	}
	
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	private static <K,V> Consumed<K,V> Consumed(Serde<K> keySerde, Class<V> valueCls) {
		return Consumed.with(keySerde, GsonUtils.getSerde(valueCls))
						.withTimestampExtractor(TS_EXTRACTOR)
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST);
	}
	
	public Topology build() {
		Objects.requireNonNull(m_topicNodeTracks, "NodeTracksTopic is null");
		Objects.requireNonNull(m_topicZoneTracks, "ZoneTrack event topic is null");
		
		StreamsBuilder builder = new StreamsBuilder();
		
		builder.addStateStore(Stores.keyValueStoreBuilder(
											Stores.persistentKeyValueStore(STORE_ZONE_LOCATIONS),
											Serdes.String(), GsonUtils.getSerde(TrackZoneLocations.class)));
		if ( m_topicZoneResidents != null ) {
			builder.addStateStore(Stores.keyValueStoreBuilder(
												Stores.persistentKeyValueStore(STORE_ZONE_RESIDENTS),
												Serdes.String(), GsonUtils.getSerde(Residents.class)));
		}
		
		KStream<String,ZoneEvent> zoneEvents =
			builder
				// DNA node에서 물체의 위치 이벤트를 수신.
				.stream(m_topicNodeTracks, Consumed(KEY_SERDE, NodeTrack.class))
				// 동일 물체에 대한 연속된 2개의 위치 정보로 부터 물체의 이동 line에 이벤트 생성. 
				.flatTransformValues(ToLineTransform<NodeTrack>::new)
				// line 정보와 물체가 검출된 node에 정의된 각 zone과의 위상 정보 이벤트를 생성.
				.flatMapValues(new ZoneEventGenerator(m_zones))
				// track별로 소속 zone 관리 및 종료시 등록된 모든 zone에서 leave하는 이벤트 생성
				.flatTransformValues(() -> new TrackZoneLocationsUpdater(STORE_ZONE_LOCATIONS));
		
		zoneEvents.to(m_topicZoneTracks,
						Produced.with(Serdes.String(), GsonUtils.getSerde(ZoneEvent.class))
								.withName("sink-global-objects"));
		
		if ( m_topicZoneResidents != null ) {
			zoneEvents
				.flatTransformValues(() -> new ResidentsGenerator(STORE_ZONE_RESIDENTS))
				.to(m_topicZoneResidents,
					Produced.with(Serdes.String(), GsonUtils.getSerde(Residents.class))
					.withName("sink-zone-residents"));
		}
		
		return builder.build();
	}
}
