package jarvey.streams.assoc;

import javax.annotation.Nullable;

import org.apache.kafka.common.serialization.Serde;
import org.locationtech.jts.geom.Point;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import utils.geo.util.GeoUtils;

import jarvey.streams.model.ObjectTrack;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.serialization.json.GsonSerde;
import jarvey.streams.serialization.json.GsonUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class LocalTrack implements ObjectTrack {
	private final static GsonSerde<LocalTrack> SERDE = GsonUtils.getSerde(LocalTrack.class);
	
	@SerializedName("node") private final String m_nodeId;
	@SerializedName("track_id") private final String m_trackId;
	@SerializedName("location") private final @Nullable Point m_location;
	@SerializedName("first_ts") private final long m_firstTs;
	@SerializedName("ts") private final long m_ts;
	
	public static final Serde<LocalTrack> getGsonSerde() {
		return SERDE;
	}
	
	public static LocalTrack from(NodeTrack track) {
		return new LocalTrack(track);
	}
	
	private LocalTrack(NodeTrack track) {
		m_nodeId = track.getNodeId();
		m_trackId = track.getTrackId();
		m_location = (track.isDeleted()) ? null : track.getLocation();
		m_firstTs = track.getFirstTimestamp();
		m_ts = track.getTimestamp();
	}
	
	@Override
	public String getKey() {
		return getTrackletId().toString();
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
	
	public boolean isDeleted() {
		return m_location == null;
	}

	@Override
	public Point getLocation() {
		return m_location;
	}
	
	public long getFirstTimestamp() {
		return m_firstTs;
	}
	
	@Override
	public long getTimestamp() {
		return m_ts;
	}
	
	public boolean isSameTrack(LocalTrack other) {
		return Objects.equal(m_nodeId, other.m_nodeId)
				&& Objects.equal(m_trackId, other.m_trackId);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		LocalTrack other = (LocalTrack)obj;
		
		return Objects.equal(m_nodeId, other.m_nodeId)
				&& Objects.equal(m_trackId, other.m_trackId)
				&& Objects.equal(m_ts, other.m_ts)
				&& Objects.equal(m_location, other.m_location);
	}
	
	@Override
	public String toString() {
		if ( m_location == null ) {	// deleted track
			return String.format("%s:deleted#%d", getTrackletId(), getTimestamp());
		}
		return String.format("%s[%s]:%s#%d",
							m_nodeId, m_trackId,  GeoUtils.toString(m_location), m_ts);
	}
}