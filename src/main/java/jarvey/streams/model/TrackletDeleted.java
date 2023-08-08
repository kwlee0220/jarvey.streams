package jarvey.streams.model;


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
	public String toString() {
		return String.format("%s[%s]#%d", getClass().getSimpleName(), m_id,  m_ts);
	}
}