package jarvey.streams.process;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.model.GUID;
import jarvey.streams.model.GlobalZoneId;
import jarvey.streams.model.LocationChanged;
import jarvey.streams.model.ObjectTrack;
import jarvey.streams.model.ResidentChanged;
import jarvey.streams.model.Residents;
import jarvey.streams.model.ZoneLineCross;
import jarvey.streams.model.ZoneLocations;
import jarvey.streams.serialization.json.GsonUtils;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TrackTopologyBuilder {
	private String m_topicNodeTracks = "node-tracks";
	private String m_topicLocationEvents = "location-events";
	private String m_storeZoneLocations = null;
	private String m_storeZoneResidents = null;
	private JdbcProcessor m_jdbc;
	
	private static final Serde<String> KEY_SERDE = Serdes.String();
	private static final Serde<GUID> GUID_SERDE = GUID.getSerde();
	private static final Serde<GlobalZoneId> GZONE_SERDE = GlobalZoneId.getSerde();
	
	private TrackTopologyBuilder() {
	}
	
	public static TrackTopologyBuilder create() {
		return new TrackTopologyBuilder();
	}
	
	public TrackTopologyBuilder setNodeTracksTopic(String topic) {
		Objects.requireNonNull(topic, "NodeTracksTopic is null");
		
		m_topicNodeTracks = topic;
		return this;
	}
	
	public TrackTopologyBuilder setLocationEventsTopic(String topic) {
		Objects.requireNonNull(topic, "LocationEventsTopic is null");
		
		m_topicLocationEvents = topic;
		return this;
	}
	
	public TrackTopologyBuilder setZoneLocationsName(String name) {
		m_storeZoneLocations = name;
		return this;
	}
	
	public TrackTopologyBuilder setZoneResidentsName(String name) {
		m_storeZoneResidents = name;
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
						.withTimestampExtractor(TS_EXTRACTOR);
	}
	
	public Topology build() {
		Objects.requireNonNull(m_topicNodeTracks, "NodeTracksTopic is null");
		Objects.requireNonNull(m_topicLocationEvents, "LocationEventsTopic is null");
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String,ZoneLineCross> zoneCrosses
			= builder.stream(m_topicNodeTracks, Consumed(KEY_SERDE, ObjectTrack.class))
					.flatMapValues(new ToLineTransform())
					.flatMapValues(new ZoneLineCrossTransform(m_jdbc));
//		zoneCrosses.print(Printed.<String, ZoneLineCross>toSysOut().withLabel("zone_crosses"));
		zoneCrosses.to(m_topicLocationEvents, Produced.with(KEY_SERDE, GsonUtils.getSerde(ZoneLineCross.class)));
		
		if ( m_storeZoneLocations != null ) {
			KStream<String,LocationChanged> zoneLocations
				= zoneCrosses.filter((k,v) -> v.isEntered() || v.isLeft() || v.isDeleted() )
							.groupBy((k,v) -> v.getGUID(),
									Grouped.with(GUID_SERDE, GsonUtils.getSerde(ZoneLineCross.class)))
							.aggregate(ZoneLocations::new, (k, cross, loc) -> loc.update(cross),
										Materialized.<GUID, ZoneLocations, KeyValueStore<Bytes,byte[]>>
													as(m_storeZoneLocations)
													.withKeySerde(GUID_SERDE)
													.withValueSerde(GsonUtils.getSerde(ZoneLocations.class))
													.withCachingDisabled())
							.toStream()
							.map((k,v) -> KeyValue.pair(k.getNodeId(), LocationChanged.from(k, v)));
//			zoneLocations.print(Printed.<String, LocationChanged>toSysOut().withLabel(m_storeZoneLocations));
			zoneLocations.to(m_storeZoneLocations, Produced.with(KEY_SERDE, GsonUtils.getSerde(LocationChanged.class)));
		}

		if ( m_storeZoneResidents != null ) {
			KStream<String,ResidentChanged> residentChanges
				= zoneCrosses.filter((k,v) -> v.isEntered() || v.isLeft() )
							.groupBy((k,v) -> v.getGlobalZoneId(),
									Grouped.with(GZONE_SERDE, GsonUtils.getSerde(ZoneLineCross.class)))
							.aggregate(Residents::new, (gz, cross, residents) -> residents.update(cross),
										Materialized.<GlobalZoneId, Residents, KeyValueStore<Bytes,byte[]>>
													as(m_storeZoneResidents)
													.withKeySerde(GZONE_SERDE)
													.withValueSerde(GsonUtils.getSerde(Residents.class))
													.withCachingDisabled())
							.toStream()
							.map((k,v) -> KeyValue.pair(k.getNodeId(), ResidentChanged.from(k, v)));
//			residentChanges.print(Printed.<String, ResidentChanged>toSysOut().withLabel(m_storeZoneResidents));
			residentChanges.to(m_storeZoneResidents, Produced.with(KEY_SERDE, GsonUtils.getSerde(ResidentChanged.class)));
		}
		
		return builder.build();
	}
}
