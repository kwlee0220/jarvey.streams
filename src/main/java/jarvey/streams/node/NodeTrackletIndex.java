package jarvey.streams.node;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;
import jarvey.streams.model.ZoneRelation;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class NodeTrackletIndex implements Timestamped {
	@SerializedName("node") private String m_node;
	@SerializedName("track_id") private String m_trackId;
	@Nullable @SerializedName("enter_zone") private String m_enterZone;
	@Nullable @SerializedName("exit_zone") private String m_exitZone;
	@SerializedName("first_ts") private long m_firstTs;
	@SerializedName("last_ts") private long m_lastTs;
	@SerializedName("partition") private int m_partition;
	@SerializedName("first_offset") private long m_firstOffset;
	@SerializedName("last_offset") private long m_lastOffset;
	@SerializedName("count") private int m_count;
	
	NodeTrackletIndex(NodeTrack track,  int partitioNo, long firstOffset) {
		m_node = track.getNodeId();
		m_trackId = track.getTrackId();
		m_firstTs = track.getFirstTimestamp();
		m_lastTs = -1;
		m_partition = partitioNo;
		m_firstOffset = firstOffset;
		m_lastOffset = -1;
		m_count = 1;
		
		ZoneRelation rel = track.getZoneRelation();
		if ( rel != null ) {
			m_enterZone = rel.getZoneId();
			m_exitZone = rel.getZoneId();
		}
	}
	
	NodeTrackletIndex(String node, String trackId, String enterZone, String exitZone,
						long firstTs, long lastTs,
						int partitioNo, long firstOffset, long lastOffset, int count) {
		m_node = node;
		m_trackId = trackId;
		m_enterZone = enterZone;
		m_exitZone = exitZone;
		m_firstTs = firstTs;
		m_lastTs = lastTs;
		m_partition = partitioNo;
		m_firstOffset = firstOffset;
		m_lastOffset = lastOffset;
		m_count = count;
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
	
	public String getEnterZone() {
		return m_enterZone;
	}
	
	public String getExitZone() {
		return m_exitZone;
	}
	
	public long getFirstTimestamp() {
		return m_firstTs;
	}
	
	public long getLastTimestamp() {
		return m_lastTs;
	}
	
	public int getPartitionNumber() {
		return m_partition;
	}
	
	public long getFirstTopicOffset() {
		return m_firstOffset;
	}
	
	public long getLastTopicOffset() {
		return m_lastOffset;
	}

	@Override
	public long getTimestamp() {
		return m_lastTs;
	}
	
	public int getUpdateCount() {
		return m_count;
	}
	
	boolean update(NodeTrack track) {
		boolean zoneUpdated = false;
		
		ZoneRelation rel = track.getZoneRelation();
		if ( rel != null ) {
			String zoneId = rel.getZoneId();
			if ( zoneId != null ) {
				if ( m_enterZone == null ) {
					m_enterZone = zoneId;
					zoneUpdated = true;
				}
				if ( !zoneId.equals(m_exitZone) ) {
					m_exitZone = zoneId;
					zoneUpdated = true;
				}
			}
		}
		m_count += 1;
		
		return zoneUpdated;
	}
	
	void lastUpdate(NodeTrack track, long lastOffset) {
		m_lastOffset = lastOffset;
		m_lastTs = track.getTimestamp();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		NodeTrackletIndex other = (NodeTrackletIndex)obj;
		return Objects.equals(m_node, other.m_node);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_node);
	}
	
	@Override
	public String toString() {
		String enterZone = (m_enterZone != null) ? m_enterZone : "?";
		String exitZone = (m_exitZone != null) ? m_exitZone : "?";
		
		return String.format("%s: {zone=%s->%s, ts=[%d:%d], offsets=%d[%d:%d], count=%d}",
								getTrackletId(), enterZone, exitZone,
								m_firstTs, m_lastTs,
								m_partition, m_firstOffset, m_lastOffset, m_count);
	}
}