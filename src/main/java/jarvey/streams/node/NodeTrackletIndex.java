package jarvey.streams.node;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.Range;
import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class NodeTrackletIndex implements Timestamped {
	@SerializedName("node") private String m_node;
	@SerializedName("track_id") private String m_trackId;
	@Nullable @SerializedName("zone_sequence") private String m_zoneSequence;
	@SerializedName("overlap_area") private String m_areaId;
	@Nullable @SerializedName("association") private String m_association;
	@SerializedName("ts_range") private Range<Long> m_tsRange;
	@SerializedName("partition") private int m_partition;
	@SerializedName("offset_range") private Range<Long> m_offsetRange;
	@SerializedName("count") private int m_count;
	
	public NodeTrackletIndex(TrackletId trkId, long firstTs,  int partitioNo,
							long firstOffset, int count) {
		m_node = trkId.getNodeId();
		m_trackId = trkId.getTrackId();
		m_tsRange = Range.atLeast(firstTs);
		m_partition = partitioNo;
		m_offsetRange = Range.atLeast(firstOffset);
		m_count = count;
	}
	
	public NodeTrackletIndex(String node, String trackId, String zoneSeq, Range<Long> tsRange, 
							int partitioNo, Range<Long> offsetRange, int count) {
		m_node = node;
		m_trackId = trackId;
		m_zoneSequence = zoneSeq;
		m_partition = partitioNo;
		m_offsetRange = offsetRange;
		m_tsRange = tsRange;
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
	
	public String getZoneSequence() {
		return m_zoneSequence;
	}
	
	public String getOverlapAreaId() {
		return m_areaId;
	}
	
	public String getAssociation() {
		return m_association;
	}
	
	public Range<Long> getTimestampRange() {
		return m_tsRange;
	}
	
	public int getPartitionNumber() {
		return m_partition;
	}
	
	public Range<Long> getTopicOffsetRange() {
		return m_offsetRange;
	}

	@Override
	public long getTimestamp() {
		return m_tsRange.max();
	}
	
	public int getUpdateCount() {
		return m_count;
	}
	
	public boolean isClosed() {
		return !m_offsetRange.isInfiniteMax();
	}
	
	void incrementCount() {
		m_count += 1;
	}
	
	void setLastUpdate(String zoneSeq, long lastTs, long lastOffset) {
		m_zoneSequence = zoneSeq;
		m_tsRange.expand(lastTs);
		m_offsetRange.expand(lastOffset);
		m_count += 1;
	}
	
	public String getEnterZone() {
		return (m_zoneSequence != null) ? m_zoneSequence.substring(1, 2) : null;
	}
	
	public String getExitZone() {
		int length = m_zoneSequence.length();
		return (m_zoneSequence != null) ? m_zoneSequence.substring(length-2, length-1) : null;
	}
	
	public void setMotionAssociation(String areaId, String association) {
		m_areaId = areaId;
		m_association = association;
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
		return String.format("%s: {part=%d, offsets=%s, ts=%s, zone_seq=%s, length=%d}",
								getTrackletId(), m_partition, m_offsetRange, m_tsRange,
								""+m_zoneSequence, m_count);
	}
}