package jarvey.streams.assoc;

import jarvey.streams.model.TrackletId;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationStatus {
	private final TrackletId m_trackletId;
	private long m_ts;
	private boolean m_associated = false;
	
	public AssociationStatus(TrackletId trkId, long ts) {
		m_trackletId = trkId;
		m_ts = ts;
		m_associated = false;
	}
	
	public TrackletId getTrackletId() {
		return m_trackletId;
	}
	
	public long getFirstTimestamp() {
		return m_ts;
	}
	
	public boolean isAssociated() {
		return m_associated;
	}
	
	public void markAssociated() {
		m_associated = true;
	}
	
	@Override
	public String toString() {
		String statusStr = m_associated ? "A" : "U";
		return String.format("%s[%s]", m_trackletId, statusStr);
	}
}
