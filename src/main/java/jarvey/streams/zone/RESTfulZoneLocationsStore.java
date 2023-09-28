package jarvey.streams.zone;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.func.KeyValue;
import utils.stream.FStream;

import jarvey.streams.serialization.json.GsonUtils;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RESTfulZoneLocationsStore implements ZoneLocationsStore {
	private final static Logger s_logger = LoggerFactory.getLogger(RESTfulZoneLocationsStore.class);

	private final HostInfo m_hostInfo;
	private final String m_storeName;
	
	RESTfulZoneLocationsStore(HostInfo hostInfo, String storeName) {
		m_hostInfo = hostInfo;
		m_storeName = storeName;
	}
	
	@Override
	public TrackZoneLocations getZoneLocations(String trackId) {
		String path = String.format("local/%s/%s", m_storeName, trackId);
		String respJson = callRemote(m_hostInfo, path);
		return (respJson != null) ? GsonUtils.parseJson(respJson, TrackZoneLocations.class) : null;
	}

	@Override
	public List<KeyValue<String,TrackZoneLocations>> getZoneLocationsAll() {
		String path = String.format("local/%s", m_storeName);
		String respJson = callRemote(m_hostInfo, path);
		
		return FStream.from(GsonUtils.parseKVList(respJson, String.class, TrackZoneLocations.class))
						.map(kv -> KeyValue.of(kv.getKey(), kv.getValue()))
						.toList();
	}
	
	private String callRemote(HostInfo hostInfo, String path) {
		OkHttpClient client = new OkHttpClient();
		
		String url = String.format("http://%s:%d/%s", hostInfo.host(), hostInfo.port(), path);
		s_logger.debug("call RESTful service: url={}", url);
		
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
		catch ( IOException e ) {
			throw new RuntimeException(String.format("fails to HTTP call: url=%s", url));
		}
	}
}
