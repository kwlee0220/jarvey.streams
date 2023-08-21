package jarvey.streams.node;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.TrackletId;
import jarvey.streams.updatelog.KeyedUpdate;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TrackFeature implements KeyedUpdate, Comparable<TrackFeature> {
	@SerializedName("node") private String m_node;
	@SerializedName("track_id") private String m_trackId;
	@Nullable @SerializedName("feature") private float[] m_feature;
	@Nullable @SerializedName("zone_relation") private String m_zoneRelation;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	public TrackFeature(String node, String trackId, float[] feature, String zoneRelation,
						long frameIndex, long ts) {
		m_node = node;
		m_trackId = trackId;
		m_feature = feature;
		m_zoneRelation = zoneRelation;
		m_frameIndex = frameIndex;
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

	@Override
	public boolean isLastUpdate() {
		return "D".equals(m_zoneRelation);
	}
	
	public float[] getFeature() {
		return m_feature;
	}
	
	public String getZoneRelation() {
		return m_zoneRelation;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public int compareTo(TrackFeature o) {
		int cmp = Long.compare(m_ts, o.m_ts);
		if ( cmp != 0 ) {
			return cmp;
		}
		
		return getTrackletId().compareTo(o.getTrackletId());
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(m_ts, getTrackId());
	}
	
	@Override
	public String toString() {
		String trackStr = (isLastUpdate()) ? "Deleted" : "Feature";
		return String.format("%s[node=%s, track_id=%s, zone=%s, frame_idx=%d, ts=%d]",
								trackStr, m_node, m_trackId, m_zoneRelation, m_frameIndex, m_ts);
	}
}