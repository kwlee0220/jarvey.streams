package jarvey.streams.model;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TrackletId implements Comparable<TrackletId> {
	@SerializedName("node") private final String m_node;
	@SerializedName("track") private final String m_trackId;
	
	public TrackletId(String node, String trackId) {
		m_node = node;
		m_trackId = trackId;
	}
	
	public String getNodeId() {
		return m_node;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		TrackletId other = (TrackletId)obj;
		return Objects.equals(m_node, other.m_node)
				&& Objects.equals(m_trackId, other.m_trackId);
	}
	
	@Override
	public int compareTo(TrackletId o) {
		int cmp = m_node.compareTo(o.m_node);
		if ( cmp != 0 ) {
			return cmp;
		}
		
		return Long.compare(Long.parseLong(m_trackId), Long.parseLong(o.m_trackId));
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_node, m_trackId);
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", m_node, m_trackId);
	}
	
	public static TrackletId fromString(String str) {
		int idx = str.lastIndexOf('[');
		String nodeId = str.substring(0, idx);
		String trackId = str.substring(idx+1, str.lastIndexOf(']'));
		
		return new TrackletId(nodeId, trackId);
	}
}