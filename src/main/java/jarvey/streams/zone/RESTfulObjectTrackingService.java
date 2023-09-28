package jarvey.streams.zone;

import java.io.IOException;
import java.util.List;

import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.stream.FStream;

import jarvey.streams.serialization.json.GsonKeyValue;
import jarvey.streams.serialization.json.GsonUtils;

import io.javalin.Javalin;
import io.javalin.http.Context;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RESTfulObjectTrackingService {
	private final static Logger s_logger = LoggerFactory.getLogger(RESTfulObjectTrackingService.class);
	private static final String STORE_ZONE_LOCATIONS = "zone-locations";
	private static final String STORE_ZONE_RESIDENTS = "zone-residents";

	private final HostInfo m_hostInfo;
	private final KafkaStreams m_streams;
	private final LocalZoneResidentStore m_residentsStore;
	private final LocalZoneLocationsStore m_locationStore;
	
	public RESTfulObjectTrackingService(HostInfo hostInfo, KafkaStreams streams) {
		m_hostInfo = hostInfo;
		m_streams = streams;
		m_residentsStore = new LocalZoneResidentStore(streams, STORE_ZONE_RESIDENTS);
		m_locationStore = new LocalZoneLocationsStore(streams, STORE_ZONE_LOCATIONS);
	}
	
	public void start() {
		Javalin app = Javalin.create().start(m_hostInfo.port());
		
		s_logger.info("starting RESTful service for store={}", STORE_ZONE_LOCATIONS);
		app.get("/zone-locations", this::getLocationsAll);
		app.get("/zone-locations/{track}", this::getLocationsOfObject);
		app.get("/local/zone-locations", this::getLocalLocationsAll);
		app.get("/local/zone-locations/{node}/{luid}", this::getLocalLocationsOfObject);
		
		s_logger.info("starting RESTful service for store={}", STORE_ZONE_RESIDENTS);
		app.get("/zone-residents", this::getResidentsAll);
		app.get("/zone-residents/{zone}", this::getResidentsOfZone);
		app.get("/local/zone-residents", this::getLocalResidentsAll);
		app.get("/local/zone-residents/{node}/{luid}", this::getLocalResidentsOfZone);
	}
	
	private void getResidentsOfZone(Context ctx) throws IOException {
		String zoneId = ctx.pathParam("zone");
		
		KeyQueryMetadata meta = m_streams.queryMetadataForKey(STORE_ZONE_RESIDENTS, zoneId,
																Serdes.String().serializer());
		Residents residents = getZoneResidentsStore(meta.activeHost()).getResidentsOfZone(zoneId);
		if ( residents != null ) {
			ctx.json(GsonUtils.toJson(residents));
		}
		else {
			ctx.status(404);
		}
	}
	
	private void getResidentsAll(Context ctx) throws IOException {
		List<GsonKeyValue<String,Residents>> result = Lists.newArrayList();
		for ( StreamsMetadata meta: m_streams.allMetadataForStore(STORE_ZONE_RESIDENTS) ) {
			ZoneResidentsStore store = getZoneResidentsStore(meta.hostInfo());
			FStream.from(store.getResidentsAll())
					.map(kv -> GsonKeyValue.of(kv.key(), kv.value()))
					.toCollection(result);
		}
		ctx.result(GsonUtils.toJson(result));
	}
	
	private void getLocalResidentsOfZone(Context ctx) throws IOException {
		String zoneId = ctx.pathParam("zone");
		
		Residents residents = m_residentsStore.getResidentsOfZone(zoneId);
		if ( residents != null ) {
			ctx.result(GsonUtils.toJson(residents));
		}
		else {
			ctx.status(404);
		}
	}
	
	private void getLocalResidentsAll(Context ctx) throws IOException {
		List<GsonKeyValue<String,Residents>> result = Lists.newArrayList();
		FStream.from(m_residentsStore.getResidentsAll())
				.map(kv -> GsonKeyValue.of(kv.key(), kv.value()))
				.toCollection(result);
		ctx.result(GsonUtils.toJson(result));
	}
	
	private ZoneResidentsStore getZoneResidentsStore(HostInfo info) {
		return ( m_hostInfo.equals(info) )
				? m_residentsStore
				:  new RESTfulZoneResidentsStore(info, STORE_ZONE_RESIDENTS);
	}
	

	
	private void getLocationsOfObject(Context ctx) throws IOException {
		String trackId = ctx.pathParam("track_id");
		
		KeyQueryMetadata meta = m_streams.queryMetadataForKey(STORE_ZONE_LOCATIONS, trackId,
																Serdes.String().serializer());
		TrackZoneLocations locs = getZoneLocationsStore(meta.activeHost()).getZoneLocations(trackId);
		if ( locs != null ) {
			ctx.result(GsonUtils.toJson(locs));
		}
		else {
			ctx.status(404);
		}
	}
	
	private void getLocationsAll(Context ctx) throws IOException {
		List<GsonKeyValue<String,TrackZoneLocations>> result = Lists.newArrayList();
		for ( StreamsMetadata meta: m_streams.allMetadataForStore(STORE_ZONE_LOCATIONS) ) {
			ZoneLocationsStore store = getZoneLocationsStore(meta.hostInfo());
			FStream.from(store.getZoneLocationsAll())
					.map(kv -> GsonKeyValue.of(kv.key(), kv.value()))
					.toCollection(result);
		}
		ctx.result(GsonUtils.toJson(result));
	}
	
	private void getLocalLocationsOfObject(Context ctx) throws IOException {
		String trackId = ctx.pathParam("track_id");
		
		TrackZoneLocations locs = m_locationStore.getZoneLocations(trackId);
		if ( locs != null ) {
			ctx.json(GsonUtils.toJson(locs));
		}
		else {
			ctx.status(404);
		}
	}
	
	private void getLocalLocationsAll(Context ctx) {
		String json = GsonUtils.toJson(FStream.from(m_locationStore.getZoneLocationsAll())
												.map(kv -> GsonKeyValue.of(kv.key(), kv.value()))
												.toList());
		ctx.result(json);
	}
	
	private ZoneLocationsStore getZoneLocationsStore(HostInfo info) {
		return ( m_hostInfo.equals(info) )
				? m_locationStore
				:  new RESTfulZoneLocationsStore(info, STORE_ZONE_LOCATIONS);
	}
}
