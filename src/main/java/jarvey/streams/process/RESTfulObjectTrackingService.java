/**
 * 
 */
package jarvey.streams.process;

import java.io.IOException;
import java.util.List;

import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.Javalin;
import io.javalin.http.Context;
import jarvey.streams.model.GlobalZoneId;
import jarvey.streams.model.ResidentChanged;
import jarvey.streams.model.Residents;
import jarvey.streams.serialization.json.GsonKeyValue;
import jarvey.streams.serialization.json.GsonUtils;
import utils.func.KeyValue;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RESTfulObjectTrackingService {
	private final static Logger s_logger = LoggerFactory.getLogger(RESTfulObjectTrackingService.class);

	private final HostInfo m_hostInfo;
	private final KafkaStreams m_streams;
	private final String m_zoneLocationStoreName;
	private final String m_zoneResidentStoreName;
	private final LocalZoneResidentStore m_residentsStore;
	
	RESTfulObjectTrackingService(HostInfo hostInfo, KafkaStreams streams,
								String zoneLocationsStoreName, String zoneResidentStoreName) {
		m_hostInfo = hostInfo;
		m_streams = streams;
		m_zoneLocationStoreName = zoneLocationsStoreName;
		m_zoneResidentStoreName = zoneResidentStoreName;
		m_residentsStore = (zoneResidentStoreName != null)
							? new LocalZoneResidentStore(streams, zoneResidentStoreName) : null;
	}
	
	void start() {
		Javalin app = Javalin.create().start(m_hostInfo.port());
		
		if ( m_zoneLocationStoreName != null ) {
			
		}
		
		if ( m_zoneResidentStoreName != null ) {
			app.get("/residents/{node}/{zone}", this::getResidentsInZone);
			app.get("/residents/{node}", this::getResidentsInNode);
			app.get("/residents", this::getResidentsAll);
			
			app.get("/local/residents", this::getLocalResidentsAll);
			app.get("/local/residents/{node}", this::getLocalResidentsInNode);
			app.get("/local/residents/{node}/{zone}", this::getLocalResidentsInZone);
		}
	}
	
	private void getResidentsInZone(Context ctx) throws IOException {
		String nodeId = ctx.pathParam("node");
		String zoneId = ctx.pathParam("zone");
		GlobalZoneId gzone = new GlobalZoneId(nodeId, zoneId);
		
		KeyQueryMetadata meta = m_streams.queryMetadataForKey(m_zoneResidentStoreName, gzone,
																GlobalZoneId.getSerde().serializer());
		Residents residents = ( m_hostInfo.equals(meta.activeHost()) )
							? m_residentsStore.getResidentsInZone(gzone)
							: new RESTfulZoneResidentStore(meta.activeHost(), m_zoneResidentStoreName).getResidentsInZone(gzone);
		if ( residents != null ) {
			ctx.json(residents.getLuids());
		}
		else {
			ctx.status(404);
		}
	}
	
	private void getResidentsInNode(Context ctx) throws IOException {
		String nodeId = ctx.pathParam("node");
		
		List<KeyValue<GlobalZoneId,Residents>> residentGroups = Lists.newArrayList();
		for ( StreamsMetadata meta: m_streams.allMetadataForStore(m_zoneResidentStoreName) ) {
			List<KeyValue<GlobalZoneId,Residents>> group
				= ( m_hostInfo.equals(meta.hostInfo()) )
					? m_residentsStore.getResidentsInNode(nodeId)
					: new RESTfulZoneResidentStore(meta.hostInfo(), m_zoneResidentStoreName).getResidentsInNode(nodeId);
			residentGroups.addAll(group);
		}
		
		List<GsonKeyValue<GlobalZoneId,Residents>> result
			= FStream.from(residentGroups)
					.map(kv -> GsonKeyValue.of(kv.key(), kv.value()))
					.toList();
		ctx.json(result);
	}
	
	private void getResidentsAll(Context ctx) throws IOException {
		List<KeyValue<GlobalZoneId,Residents>> residentGroups = Lists.newArrayList();
		for ( StreamsMetadata meta: m_streams.allMetadataForStore(m_zoneResidentStoreName) ) {
			List<KeyValue<GlobalZoneId,Residents>> group
				= ( m_hostInfo.equals(meta.hostInfo()) )
					? m_residentsStore.getResidentsAll()
					: new RESTfulZoneResidentStore(meta.hostInfo(), m_zoneResidentStoreName).getResidentsAll();
			residentGroups.addAll(group);
		}
		
		List<ResidentChanged> result
			= FStream.from(residentGroups)
					.map(kv -> ResidentChanged.from(kv.key(), kv.value()))
					.toList();
		ctx.result(GsonUtils.toJson(result));
	}
	
	private void getLocalResidentsInZone(Context ctx) throws IOException {
		String nodeId = ctx.pathParam("node");
		String zoneId = ctx.pathParam("zone");
		GlobalZoneId gzone = new GlobalZoneId(nodeId, zoneId);
		
		Residents residents = m_residentsStore.getResidentsInZone(gzone);
		if ( residents != null ) {
			ctx.json(residents);
		}
		else {
			ctx.status(404);
		}
	}
	
	private void getLocalResidentsInNode(Context ctx) throws IOException {
		String nodeId = ctx.pathParam("node");
		
		ctx.json(FStream.from(m_residentsStore.getResidentsInNode(nodeId))
						.map(kv -> GsonKeyValue.of(kv.key(), kv.value()))
						.toList());
	}
	
	public void getLocalResidentsAll(Context ctx) throws IOException {
		ctx.json(FStream.from(m_residentsStore.getResidentsAll())
						.map(kv -> GsonKeyValue.of(kv.key(), kv.value()))
						.toList());
	}
}
