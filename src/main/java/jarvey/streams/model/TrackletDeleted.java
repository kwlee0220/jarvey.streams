package jarvey.streams.model;

import com.google.common.base.Objects;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class TrackletDeleted implements Timestamped {
	private final TrackletId m_id;
	private final long m_ts;
	
	public static TrackletDeleted of(TrackletId trackId, long ts) {
		return new TrackletDeleted(trackId, ts);
	}
	
	private TrackletDeleted(TrackletId trackId, long ts) {
		m_id = trackId;
		m_ts = ts;
	}
	
	public TrackletId getTrackletId() {
		return m_id;
	}
	
	@Override
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		TrackletDeleted other = (TrackletDeleted)obj;
		return Objects.equal(m_id, other.m_id) && Objects.equal(m_ts, other.m_ts);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(m_id, m_ts);
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]#%d", getClass().getSimpleName(), m_id,  m_ts);
	}
}