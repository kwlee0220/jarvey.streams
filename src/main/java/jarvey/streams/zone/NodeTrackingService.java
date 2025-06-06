package jarvey.streams.zone;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.Javalin;
import io.javalin.http.Context;

import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.streams.model.TrackletId;
import jarvey.streams.serialization.json.GsonUtils;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NodeTrackingService {
	private final static Logger s_logger = LoggerFactory.getLogger(NodeTrackingService.class);
	
	private final HostInfo m_hostInfo;
	private final KafkaStreams m_streams;
	private final JdbcProcessor m_jdbc;
	private final String m_zoneLocations;
	private final String m_zoneResidents;
	
	NodeTrackingService(HostInfo hostInfo, KafkaStreams streams, JdbcProcessor jdbc,
						String zoneLocations, String zoneResidents) {
		m_hostInfo = hostInfo;
		m_streams = streams;
		m_jdbc = jdbc;
		m_zoneLocations = zoneLocations;
		m_zoneResidents = zoneResidents;
	}
	
	void start() {
		Javalin app = Javalin.create().start(m_hostInfo.port());
		
//		app.get("/residents", this::getResidentsAll);
//		app.get("/residents/{node}", this::getResidentsInNode);
		app.get("/residents/{node}/{zone}", this::getResidentsInZone);
		
		app.get("/locations/{node}/{luid}", this::getZonesOfObject);
	}
	
	void getZonesOfObject(Context ctx) {
		String nodeId = ctx.pathParam("node");
		String trackId = ctx.pathParam("track_id");
		TrackletId guid = new TrackletId(nodeId, trackId);
		
		KeyQueryMetadata meta = m_streams.queryMetadataForKey(m_zoneLocations, guid,
															GsonUtils.getSerde(TrackletId.class).serializer());
		if ( m_hostInfo.equals(meta.activeHost()) ) {
			TrackZoneLocations location = getZoneLocationsStore().get(guid);
			if ( location != null ) {
				ctx.json(location.getZoneIds());
			}
			else {
				ctx.status(404);
			}
			return;
		}

		try {
			String path = String.format("%s/%s/%s", m_zoneLocations, guid.getNodeId(), guid.getTrackId());
			ctx.result(callRemote(meta.activeHost(), path));
		}
		catch ( IOException e1 ) {
			ctx.status(500);
		}
	}
	
//	void getResidentsInNode(Context ctx) {
//		String nodeId = ctx.pathParam("node");
//		
//		Map<String, Map<String, Polygon>> zoneGroups = ZoneLineCrossTransform.loadZoneGroups(m_jdbc);
//		Map<String, List<String>> zoneIdGroups
//				= FStream.from(zoneGroups)
//							.mapValue(grp -> (List<String>)FStream.from(grp.keySet()).toList())
//							.toMap();
//		
//		for ( StreamsMetadata meta: m_streams.allMetadataForStore(m_zoneResidents) ) {
//			if ( !m_hostInfo.equals(meta.hostInfo()) ) {
//				FStream.from(getResidentsStore().all())
//						.filter(kv -> kv.key.getNodeId().equals(nodeId))
//						.toKeyValueStream(kv -> KeyValue.of(kv.key, kv.value))
//						.toMap();
//			}
//			count += fetchCountFromRemoteInstance(meta.hostInfo().host(), meta.hostInfo().port());
//		}
//		
//		KeyQueryMetadata meta = m_streams.queryMetadataForKey(m_zoneResidents, gzone,
//															GlobalZoneId.getSerde().serializer());
//		if ( m_hostInfo.equals(meta.activeHost()) ) {
//			Residents residents = getResidentsStore().get(gzone);
//			if ( residents != null ) {
//				ctx.json(residents.getResidents());
//			}
//			else {
//				ctx.status(404);
//			}
//			return;
//		}
//
//		try {
//			String path = String.format("%s/%s/%s", m_zoneLocations, gzone.getNodeId(), gzone.getZoneId());
//			ctx.result(callRemote(meta.activeHost(), path));
//		}
//		catch ( IOException e1 ) {
//			ctx.status(500);
//		}
//	}
	
	void getResidentsInZone(Context ctx) {
		String zoneId = ctx.pathParam("zone");
		
		KeyQueryMetadata meta = m_streams.queryMetadataForKey(m_zoneResidents, zoneId,
															Serdes.String().serializer());
		if ( m_hostInfo.equals(meta.activeHost()) ) {
			Residents residents = getResidentsStore().get(zoneId);
			if ( residents != null ) {
				ctx.json(residents.getTrackIds());
			}
			else {
				ctx.status(404);
			}
			return;
		}

		try {
			String path = String.format("%s/%s", m_zoneLocations, zoneId);
			ctx.result(callRemote(meta.activeHost(), path));
		}
		catch ( IOException e1 ) {
			ctx.status(500);
		}
	}
	
	Map<String, Residents> getResidentsLocal() {
		try ( KeyValueIterator<String,Residents> it = getResidentsStore().all() ) {
			return FStream.from(it)
							.toKeyValueStream(kv -> kv.key, kv -> kv.value)
                            .toMap();
		}
	}
	
//	void getResidentsAll(Context ctx, String storeName) {
//		for ( StreamsMetadata meta: m_streams.allMetadataForStore(storeName) ) {
//			if ( !m_hostInfo.equals(meta.hostInfo()) ) {
//				continue;
//			}
//			count += fetchCountFromRemoteInstance(meta.hostInfo().host(), meta.hostInfo().port());
//		}
//	}
//	
//	void getResidentCount(Context ctx) {
//		long count = getResidentsStore().approximateNumEntries();
//		for ( StreamsMetadata meta: m_streams.allMetadataForStore("residents") ) {
//			if ( !m_hostInfo.equals(meta.hostInfo()) ) {
//				continue;
//			}
//			count += fetchCountFromRemoteInstance(meta.hostInfo().host(), meta.hostInfo().port());
//		}
//		
//		ctx.json(count);
//	}
//	
//	void getResidentCount(Context ctx) {
//		long count = getResidentsStore().approximateNumEntries();
//		for ( StreamsMetadata meta: m_streams.allMetadataForStore("residents") ) {
//			if ( !m_hostInfo.equals(meta.hostInfo()) ) {
//				continue;
//			}
//			count += fetchCountFromRemoteInstance(meta.hostInfo().host(), meta.hostInfo().port());
//		}
//		
//		ctx.json(count);
//	}
	
	private String callRemote(HostInfo hostInfo, String path) throws IOException {
		OkHttpClient client = new OkHttpClient();
		
		String url = String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path);
		Request request = new Request.Builder().url(url).build();
		try (Response response = client.newCall(request).execute()) {
			return response.body().string();
		}
	}

	private long fetchCountFromRemoteInstance(String host, int port) {
		OkHttpClient client = new OkHttpClient();
		
		String url = String.format("http://%s:%d/residents/count/local", host, port);
		Request request = new Request.Builder().url(url).build();
		
		try (Response response = client.newCall(request).execute()) {
			return Long.parseLong(response.body().string());
		}
		catch ( Exception e ) {
			s_logger.error("Could not get leaderboard count", e);
			return 0L;
		}
	}
	
	private void getCountLocal(Context ctx) {
		long count = 0L;
		try {
			count = getResidentsStore().approximateNumEntries();
		}
		catch ( Exception e ) {
			s_logger.error("Could not get local leaderboard count", e);
		}
		finally {
			ctx.result(String.valueOf(count));
		}
	}
	
	private ReadOnlyKeyValueStore<String,Residents> getResidentsStore() {
		return m_streams.store(StoreQueryParameters.fromNameAndType(m_zoneResidents,
																	QueryableStoreTypes.keyValueStore()));
	}
	
	private ReadOnlyKeyValueStore<TrackletId,TrackZoneLocations> getZoneLocationsStore() {
		return m_streams.store(StoreQueryParameters.fromNameAndType(m_zoneLocations,
																	QueryableStoreTypes.keyValueStore()));
	}
	
	private void execute(Context ctx, String url, BiConsumer<Context,String> handleResponse,
						BiConsumer<Context,IOException> handleException) {
		OkHttpClient client = new OkHttpClient();
		Request req = new Request.Builder().url(url).build();
		try ( Response resp = client.newCall(req).execute() ) {
			handleResponse.accept(ctx, resp.body().string());
		}
		catch ( IOException e ) {
			handleException.accept(ctx, e);
		}
	}
	
	private void execute(Context ctx, String url) {
		OkHttpClient client = new OkHttpClient();
		Request req = new Request.Builder().url(url).build();
		try ( Response resp = client.newCall(req).execute() ) {
			ctx.result(resp.body().string());
		}
		catch ( IOException e ) {
			ctx.status(500);
		}
	}
}
