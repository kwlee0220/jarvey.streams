package jarvey.streams.zone;

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

import utils.jdbc.JdbcProcessor;

import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.model.GUID;
import jarvey.streams.model.ObjectTrack;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class TrackTopologyBuilder {
	private static final Serde<String> KEY_SERDE = Serdes.String();
	private static final String STORE_LAST_TRACKS = "last-tracks";
	private static final String STORE_ZONE_LOCATIONS = "zone-locations";
	private static final String STORE_ZONE_RESIDENTS = "zone-residents";
	
	private String m_topicNodeTracks = "node-tracks";
	private String m_topicZoneLineRelations = "zone-line-relations";
	private String m_topicLocationEvents = null;
	private String m_topicZoneLocations = null;
	private String m_topicZoneResidents = null;
	private JdbcProcessor m_jdbc;
	
	private TrackTopologyBuilder() {
	}
	
	static TrackTopologyBuilder create() {
		return new TrackTopologyBuilder();
	}
	
	public TrackTopologyBuilder setNodeTracksTopic(String topic) {
		Objects.requireNonNull(topic, "NodeTracksTopic is null");
		
		m_topicNodeTracks = topic;
		return this;
	}
	
	public TrackTopologyBuilder setZoneLineRelationsTopic(String topic) {
		m_topicZoneLineRelations = topic;
		return this;
	}
	
	public TrackTopologyBuilder setLocationEventsTopic(String topic) {
		m_topicLocationEvents = topic;
		return this;
	}
	
	public TrackTopologyBuilder setZoneLocationsTopic(String topic) {
		m_topicZoneLocations = topic;
		return this;
	}
	
	public TrackTopologyBuilder setZoneResidentsTopic(String topic) {
		m_topicZoneResidents = topic;
		return this;
	}
	
	public TrackTopologyBuilder setJdbcProcessor(JdbcProcessor jdbc) {
		Objects.requireNonNull(jdbc, "JdbcProcessor is null");
		
		m_jdbc = jdbc;
		return this;
	}
	
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	private static <K,V> Consumed<K,V> Consumed(Serde<K> keySerde, Class<V> valueCls) {
		return Consumed.with(keySerde, GsonUtils.getSerde(valueCls))
						.withTimestampExtractor(TS_EXTRACTOR)
						.withOffsetResetPolicy(AutoOffsetReset.LATEST);
	}
	
	public Topology build() {
		Objects.requireNonNull(m_topicNodeTracks, "NodeTracksTopic is null");
		Objects.requireNonNull(m_topicLocationEvents, "LocationEventsTopic is null");
		
		StreamsBuilder builder = new StreamsBuilder();
		
		builder.addStateStore(Stores.keyValueStoreBuilder(
//											Stores.persistentKeyValueStore(STORE_LAST_TRACKS),
											Stores.inMemoryKeyValueStore(STORE_LAST_TRACKS),
											GUID.getSerde(), GsonUtils.getSerde(ObjectTrack.class)));
		builder.addStateStore(Stores.keyValueStoreBuilder(
											Stores.persistentKeyValueStore(STORE_ZONE_LOCATIONS),
											GUID.getSerde(), GsonUtils.getSerde(ZoneLocations.class)));
		builder.addStateStore(Stores.keyValueStoreBuilder(
											Stores.persistentKeyValueStore(STORE_ZONE_RESIDENTS),
											GlobalZoneId.getSerde(), GsonUtils.getSerde(Residents.class)));
		
		@SuppressWarnings("unchecked")
		KStream<String,MergedLocationEvent>[] branches
			// DNA node에서 물체의 위치 이벤트를 수신.
			= builder.stream(m_topicNodeTracks, Consumed(KEY_SERDE, ObjectTrack.class))
					// 동일 물체에 대한 연속된 2개의 위치 정보로 부터 물체의 이동 line에 이벤트 생성. 
					.flatTransformValues(ToLineTransform::new, STORE_LAST_TRACKS)
					// line 정보와 물체가 검출된 node에 정의된 각 zone과의 위상 정보 이벤트를 생성.
					.flatMapValues(new ZoneLineRelationDetector(m_jdbc))
					// 물체 검출/추적 오류로 인해 inconsistent한 물체 추적 이벤트가 발생하는 경우, 이를 보정함.
					.flatTransformValues(
							() -> new AdjustZoneLineCrossTransform(STORE_ZONE_LOCATIONS),
							STORE_ZONE_LOCATIONS)
					.branch((k, v) -> v.isZoneLineCrosses(),
							(k, v) -> v.isLocationChanged());
		
		KStream<String,ZoneLineRelationEvent> relations
			= branches[0].flatMapValues(merged -> merged.getZoneLineCrosses());
		
//		KStream<String,ZoneLineRelationEvent> relations2 = relations.filter((k,ev) -> ev.getLuid() == 3);
//		relations.print(Printed.<String, ZoneLineRelationEvent>toSysOut().withLabel("relations"));
		if ( m_topicZoneLineRelations != null ) {
			relations.to(m_topicZoneResidents,
						Produced.with(KEY_SERDE, GsonUtils.getSerde(ZoneLineRelationEvent.class)));
		}

		KStream<String,ZoneLineRelationEvent> locEvents
			= relations.filter((node, cross) -> cross.isEntered() || cross.isLeft()
												|| cross.isThrough() || cross.isDeleted());
//		locEvents.print(Printed.<String, ZoneLineRelationEvent>toSysOut().withLabel("location-events"));
		if ( m_topicLocationEvents != null ) {
			locEvents.to(m_topicLocationEvents,
						Produced.with(KEY_SERDE, GsonUtils.getSerde(ZoneLineRelationEvent.class)));
		}
		
		KStream<String,ResidentChanged> residentChanges
			= locEvents.flatTransformValues(() -> new ResidentChangedTransform(STORE_ZONE_RESIDENTS),
													STORE_ZONE_RESIDENTS);
//		residentChanges.print(Printed.<String, ResidentChanged>toSysOut().withLabel(m_topicZoneResidents));
		if ( m_topicZoneResidents != null ) {
			residentChanges.to(m_topicZoneResidents,
								Produced.with(KEY_SERDE, GsonUtils.getSerde(ResidentChanged.class)));
		}
		
		if ( m_topicZoneLocations != null ) {
			KStream<String,LocationChanged> locChangeds
				= branches[1].mapValues(merged -> merged.getLocationChanged());
//			locChangeds.print(Printed.<String, LocationChanged>toSysOut().withLabel("loc-changes"));
			locChangeds.to(m_topicZoneLocations,
							Produced.with(KEY_SERDE, GsonUtils.getSerde(LocationChanged.class)));
		}
		
		return builder.build();
	}
}
