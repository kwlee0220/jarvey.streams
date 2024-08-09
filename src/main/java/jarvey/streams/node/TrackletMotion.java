package jarvey.streams.node;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import utils.func.FOption;

import jarvey.streams.model.TrackletId;
import jarvey.streams.updatelog.KeyedUpdate;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TrackletMotion implements KeyedUpdate, Comparable<TrackletMotion> {
	@SerializedName("node") private String m_node;
	@SerializedName("track_id") private String m_trackId;
	@Nullable @SerializedName("zone_sequence") private String m_zoneSequence;
	@Nullable @SerializedName("enter_zone") private String m_enterZone;
	@Nullable @SerializedName("exit_zone") private String m_exitZone;
	@Nullable @SerializedName("motion") private String m_motion;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	public TrackletMotion(String node, String trackId, String zoneSequence, String enterZone,
						String exitZone, String motion, long frameIndex, long ts) {
		m_node = node;
		m_trackId = trackId;
		m_zoneSequence = zoneSequence;
		m_enterZone = enterZone;
		m_exitZone = exitZone;
		m_motion = motion;
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
		return true;
	}
	
	public String getZoneSequence() {
		return m_zoneSequence;
	}
	
	public String getEnterZone() {
		return m_enterZone;
	}
	
	public String getExitZone() {
		return m_exitZone;
	}
	
	public String getMotion() {
		return m_motion;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public int compareTo(TrackletMotion o) {
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
		String enter = FOption.getOrElse(m_enterZone, "?");
		String exit = FOption.getOrElse(m_exitZone, "?");
		return String.format("%s[%s -> %s, seq=%s, motion=%s, frame_idx=%d, ts=%d]",
								getTrackletId(), enter, exit, m_zoneSequence, m_motion,
								m_frameIndex, m_ts);
	}
}