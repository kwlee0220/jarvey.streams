package jarvey.streams.model;

import java.util.List;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Point;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import utils.geo.util.GeoUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GlobalTrack implements ObjectTrack {
	@SerializedName("node") private String m_node;
	@SerializedName("track_id") private String m_trackId;
	@Nullable @SerializedName("location") Point m_location;
	@Nullable @SerializedName("overlap_area") String m_overlapArea;
	@Nullable @SerializedName("supports") List<LocalTrack> m_supports;
	@SerializedName("ts") private long m_ts;
	
	public static final GlobalTrack deleted(LocalTrack track, String overlapArea) {
		return new GlobalTrack(track.getTrackletId(), overlapArea, null, null, track.getTimestamp());
	}
	
	public GlobalTrack(LocalTrack track) {
		this(track, null);
	}
	
	public GlobalTrack(LocalTrack track, String overlapArea) {
		m_node = track.getNodeId();
		m_trackId = track.getTrackId();
		m_location = track.getLocation();
		m_overlapArea = overlapArea;
		m_supports = null;
		m_ts = track.getTimestamp();
	}
	
	public GlobalTrack(TrackletId tracklet, String overlapArea, Point loc,
						List<LocalTrack> ltracks, long ts) {
		m_node = tracklet.getNodeId();
		m_trackId = tracklet.getTrackId();
		m_overlapArea = overlapArea;
		m_location = loc;
		m_supports = ltracks;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_node;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	public TrackletId getTrackletId() {
		return new TrackletId(m_node, m_trackId);
	}

	@Override
	public String getKey() {
		return getTrackletId().toString();
	}
	
	public boolean isDeleted() {
		return m_location == null;
	}
	
	public boolean isClustered() {
		return m_supports.size() > 1;
	}
	
	public Point getLocation() {
		return m_location;
	}
	
	public String getOverlapArea() {
		return m_overlapArea;
	}
	
	public List<LocalTrack> getLocalTracks() {
		return m_supports;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public boolean isSameTrack(GlobalTrack other) {
		return Objects.equal(m_node, other.m_node)
				&& Objects.equal(m_trackId, other.m_trackId);
	}
	
	@Override
	public String toString() {
		if ( m_location == null ) {	// deleted track
			return String.format("%s:deleted#%d", getTrackletId(), getTimestamp());
		}
		
		String locStr = m_location != null ? GeoUtils.toString(m_location, 1) : "none";
		if ( m_supports != null ) {
			String supportStr = FStream.from(m_supports)
										.map(LocalTrack::getTrackletId)
										.sort()
										.map(TrackletId::toString)
										.join('-');
			return String.format("%s:%s#%d %s {%s}",
									getTrackletId(), locStr, m_ts,
									m_overlapArea, supportStr);
		}
		else {
			String area = m_overlapArea != null ? String.format(" %s", m_overlapArea) : "";
			return String.format("%s:%s#%d%s", getTrackletId(), locStr, m_ts, area);
		}
	}
}