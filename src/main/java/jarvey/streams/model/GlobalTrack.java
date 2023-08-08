package jarvey.streams.model;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.kafka.common.serialization.Serde;
import org.locationtech.jts.geom.Point;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import jarvey.streams.serialization.json.GsonSerde;
import jarvey.streams.serialization.json.GsonUtils;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GlobalTrack implements ObjectTrack {
	private final static GsonSerde<GlobalTrack> SERDE = GsonUtils.getSerde(GlobalTrack.class);

	@SerializedName("node") private String m_nodeId;
	@SerializedName("track_id") private String m_trackId;
	@Nullable @SerializedName("location") Point m_location;
	@Nullable @SerializedName("overlap_area") String m_overlapArea;
	@Nullable @SerializedName("support") List<LocalTrack> m_ltracks;
	@SerializedName("ts") private long m_ts;
	
	public static final Serde<GlobalTrack> getGsonSerde() {
		return SERDE;
	}
	
	public static final GlobalTrack deleted(NodeTrack track) {
		return new GlobalTrack(track.getTrackletId(), null, null, null, track.getTimestamp());
	}
	
	public static final GlobalTrack deleted(LocalTrack track) {
		return new GlobalTrack(track.getTrackletId(), null, null, null, track.getTimestamp());
	}
	
	public GlobalTrack(LocalTrack track) {
		m_nodeId = track.getNodeId();
		m_trackId = track.getTrackId();
		m_location = track.getLocation();
		m_overlapArea = null;
		m_ltracks = Arrays.asList(track);
		m_ts = track.getTimestamp();
	}
	
	public GlobalTrack(TrackletId tracklet, String overlapArea, Point loc, List<LocalTrack> ltracks, long ts) {
		m_nodeId = tracklet.getNodeId();
		m_trackId = tracklet.getTrackId();
		m_overlapArea = overlapArea;
		m_location = loc;
		m_ltracks = ltracks;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	public TrackletId getTrackletId() {
		return new TrackletId(m_nodeId, m_trackId);
	}

	@Override
	public String getId() {
		return getTrackletId().toString();
	}
	
	public boolean isDeleted() {
		return m_location == null;
	}
	
	public boolean isClustered() {
		return m_ltracks.size() > 1;
	}
	
	public Point getLocation() {
		return m_location;
	}
	
	public List<LocalTrack> getLocalTracks() {
		return m_ltracks;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public boolean isSameTrack(GlobalTrack other) {
		return Objects.equal(m_nodeId, other.m_nodeId)
				&& Objects.equal(m_trackId, other.m_trackId);
	}
	
	@Override
	public String toString() {
		if ( m_location == null ) {	// deleted track
			return String.format("%s:deleted#%d", getTrackletId(), getTimestamp());
		}
		
		String locStr = m_location != null ? GeoUtils.toString(m_location, 1) : "none";
		if ( m_overlapArea != null ) {
			String supportStr = FStream.from(m_ltracks)
										.map(LocalTrack::getTrackletId)
										.sort()
										.map(TrackletId::toString)
										.join('-');
			return String.format("%s:%s#%d %s{%s}",
									getTrackletId(), locStr, m_ts,
									m_overlapArea, supportStr);
		}
		else {
			return String.format("%s:%s#%d", getTrackletId(), locStr, m_ts);
		}
	}
}