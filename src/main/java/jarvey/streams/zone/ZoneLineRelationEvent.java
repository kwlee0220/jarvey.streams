package jarvey.streams.zone;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.LineSegment;

import com.google.gson.annotations.SerializedName;

import utils.geo.util.GeoUtils;

import jarvey.streams.model.TrackletId;
import jarvey.streams.model.Timestamped;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZoneLineRelationEvent implements Timestamped {
	@SerializedName("node") private final String m_nodeId;
	@SerializedName("track_id") private final String m_trackId;
	@SerializedName("state") private final ZoneLineRelation m_relation;
	@Nullable @SerializedName("line") private final LineSegment m_line;
	@Nullable @SerializedName("zones") private final String m_zone;
	@SerializedName("frame_index") private final long m_frameIndex;
	@SerializedName("ts") private final long m_ts;
	
	public ZoneLineRelationEvent(String nodeId, String trackId, ZoneLineRelation state,
								@Nullable String zone, @Nullable LineSegment line,
								long frameIndex, long ts) {
		m_nodeId = nodeId;
		m_trackId = trackId;
		m_relation = state;
		m_zone = zone;
		m_line = line;
		m_frameIndex = frameIndex;
		m_ts = ts;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	public TrackletId getGUID() {
		return new TrackletId(m_nodeId, m_trackId);
	}
	
	public String getZone() {
		return m_zone;
	}
	
	public GlobalZoneId getGlobalZoneId() {
		return new GlobalZoneId(m_nodeId, m_zone);
	}
	
	public ZoneLineRelation getRelation() {
		return m_relation;
	}
	
	public LineSegment getLine() {
		return m_line;
	}
	
	public boolean isUnassigned() {
		return m_relation.equals(ZoneLineRelation.Unassigned);
	}
	
	public boolean isEntered() {
		return m_relation.equals(ZoneLineRelation.Entered);
	}
	
	public boolean isLeft() {
		return m_relation.equals(ZoneLineRelation.Left);
	}
	
	public boolean isInside() {
		return m_relation.equals(ZoneLineRelation.Inside);
	}
	
	public boolean isThrough() {
		return m_relation.equals(ZoneLineRelation.Through);
	}
	
	public boolean isDeleted() {
		return m_relation.equals(ZoneLineRelation.Deleted);
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public String toString() {
		if ( !isDeleted() ) {
			String zoneStr = (m_zone != null) ? String.format(", zones=%s", m_zone) : "";
			String lineStr = (m_line != null) ? String.format(", line=%s", GeoUtils.toString(m_line, 1)) : "";
			return String.format("%s[node=%s, luid=%d%s%s, frame_idx=%d, ts=%d]",
								m_relation, m_nodeId, m_trackId, zoneStr, lineStr, m_frameIndex, m_ts);
		}
		else {
			return String.format("%s[node=%s, luid=%d, frame_idx=%d, ts=%d]",
					m_relation, m_nodeId, m_trackId, m_frameIndex, m_ts);
		}
	}
}