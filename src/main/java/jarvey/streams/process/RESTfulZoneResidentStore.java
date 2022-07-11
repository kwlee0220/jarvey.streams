/**
 * 
 */
package jarvey.streams.process;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.streams.model.GlobalZoneId;
import jarvey.streams.model.Residents;
import jarvey.streams.serialization.json.GsonUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import utils.func.KeyValue;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RESTfulZoneResidentStore {
	private final static Logger s_logger = LoggerFactory.getLogger(RESTfulZoneResidentStore.class);

	private final HostInfo m_hostInfo;
	private final String m_storeName;
	
	RESTfulZoneResidentStore(HostInfo hostInfo, String storeName) {
		m_hostInfo = hostInfo;
		m_storeName = storeName;
	}
	
	public Residents getResidentsInZone(GlobalZoneId gzone) throws IOException {
		String path = String.format("local/%s/%s/%s", m_storeName, gzone.getNodeId(), gzone.getZoneId());
		String respJson = callRemote(m_hostInfo, path);
		return (respJson != null) ? GsonUtils.parseJson(respJson, Residents.class) : null;
	}
	
	public List<KeyValue<GlobalZoneId,Residents>> getResidentsInNode(String nodeId) throws IOException {
		String path = String.format("local/%s/%s", m_storeName, nodeId);
		String respJson = callRemote(m_hostInfo, path);
		
		return FStream.from(GsonUtils.parseKVList(respJson, GlobalZoneId.class, Residents.class))
						.map(kv -> KeyValue.of(kv.getKey(), kv.getValue()))
						.toList();
	}
	
	public List<KeyValue<GlobalZoneId,Residents>> getResidentsAll() throws IOException {
		String path = String.format("local/%s", m_storeName);
		String respJson = callRemote(m_hostInfo, path);
		
		return FStream.from(GsonUtils.parseKVList(respJson, GlobalZoneId.class, Residents.class))
						.map(kv -> KeyValue.of(kv.getKey(), kv.getValue()))
						.toList();
	}
	
	private String callRemote(HostInfo hostInfo, String path) throws IOException {
		OkHttpClient client = new OkHttpClient();
		
		String url = String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path);
		Request request = new Request.Builder().url(url).build();
		try (Response resp = client.newCall(request).execute()) {
			switch ( resp.code() ) {
				case 200:
					return resp.body().string();
				case 404:
					return null;
				default:
					throw new IOException("HTTP status: " + resp.code());
			}
		}
	}
}
