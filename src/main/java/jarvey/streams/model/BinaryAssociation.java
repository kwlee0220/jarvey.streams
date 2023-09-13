package jarvey.streams.model;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class BinaryAssociation implements Association {
	@SerializedName("id") private String m_id;
	@SerializedName("left") private TimedTracklet m_left;
	@SerializedName("right") private TimedTracklet m_right;
	@SerializedName("first_ts") private long m_firstTs;
	@SerializedName("score") private double m_score;
	
	private static class TimedTracklet {
		@SerializedName("tracklet") private TrackletId m_tracklet;
		@SerializedName("ts") private long m_ts;
		
		TimedTracklet(TrackletId trkId, long ts) {
			m_tracklet = trkId;
			m_ts = ts;
		}
	}
	
	public BinaryAssociation(TrackletId leftTrkId, long leftFirstTs, long leftTs,
							TrackletId rightTrkId, long rightFirstTs, long rightTs, double score) {
		if ( leftTrkId.compareTo(rightTrkId) < 0 ) {
			m_left = new TimedTracklet(leftTrkId, leftTs);
			m_right = new TimedTracklet(rightTrkId, rightTs);
			
			m_id = leftTrkId.toString();
			m_firstTs = leftFirstTs;
		}
		else {
			m_right = new TimedTracklet(leftTrkId, leftTs);
			m_left = new TimedTracklet(rightTrkId, rightTs);
			
			m_id = rightTrkId.toString();
			m_firstTs = rightFirstTs;
		}
		
		if ( leftFirstTs < rightFirstTs ) {
			m_id = leftTrkId.toString();
			m_firstTs = leftFirstTs;
		}
		else if ( leftFirstTs > rightFirstTs ) {
			m_id = rightTrkId.toString();
			m_firstTs = rightFirstTs;
		}
		m_score = score;
	}
	
	public BinaryAssociation(String id, TrackletId left, TrackletId right, double score,
								long leftTs, long rightTs, long firstTs) {
		m_id = id;
		if ( left.compareTo(right) < 0 ) {
			m_left = new TimedTracklet(left, leftTs);
			m_right = new TimedTracklet(right, rightTs);
		}
		else {
			m_id = left.toString();
			m_right = new TimedTracklet(left, leftTs);
			m_left = new TimedTracklet(right, rightTs);
		}
		m_firstTs = firstTs;
		m_score = score;
	}

	@Override
	public String getId() {
		return m_id;
	}
	
	public TrackletId getLeader() {
		return TrackletId.fromString(m_id);
	}
	
	public TrackletId getFollower() {
		if ( m_left.m_tracklet.toString().equals(m_id) ) {
			return m_right.m_tracklet;
		}
		else {
			return m_left.m_tracklet;
		}
	}

	@Override
	public Set<TrackletId> getTracklets() {
		return Sets.newHashSet(m_left.m_tracklet, m_right.m_tracklet);
	}
	
	public TrackletId getLeftTrackletId() {
		return m_left.m_tracklet;
	}
	
	public TrackletId getRightTrackletId() {
		return m_right.m_tracklet;
	}
	
	public int indexOf(TrackletId trkId) {
		if ( m_left.m_tracklet.equals(trkId) ) {
			return 0;
		}
		else if ( m_right.m_tracklet.equals(trkId) ) {
			return 1;
		}
		else {
			throw new IllegalArgumentException(String.format("invalid tracklet: %s", trkId));
		}
	}
	
	public TrackletId getOther(TrackletId trkId) {
		return trkId.equals(getLeftTrackletId()) ? getRightTrackletId() : getLeftTrackletId();
	}

	@Override
	public double getScore() {
		return m_score;
	}

	@Override
	public long getFirstTimestamp() {
		return m_firstTs;
	}
	
	public long getLeftTimestamp() {
		return m_left.m_ts;
	}
	
	public long getRightTimestamp() {
		return m_right.m_ts;
	}
	
	@Override
	public long getTimestamp() {
		return Math.max(getLeftTimestamp(), getRightTimestamp());
	}

	public boolean match(BinaryAssociation other) {
		return Objects.equals(getLeftTrackletId(), other.getLeftTrackletId())
				&& Objects.equals(getRightTrackletId(), other.getRightTrackletId());
	}
	
	@Override
	public BinaryAssociation removeTracklet(TrackletId trkId) {
		if ( getLeftTrackletId().equals(trkId) || getRightTrackletId().equals(trkId) ) {
			return null;
		}
		else {
			return this;
		}
	} 
	
	@Override
	public String toString() {
		TrackletId trkId = TrackletId.fromString(m_id);
		if ( getLeftTrackletId().equals(trkId) ) {
			TrackletId leader = getLeftTrackletId();
			return String.format("%s[*%s]-%s:%.2f#%d",
								leader.getNodeId(), leader.getTrackId(), getRightTrackletId(),
								m_score, getTimestamp());
		}
		else {
			TrackletId leader = getRightTrackletId();
			return String.format("%s-%s[*%s]:%.2f#%d",
								getLeftTrackletId(), leader.getNodeId(), leader.getTrackId(),
								m_score, getTimestamp());
		}
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(getLeftTrackletId(), getRightTrackletId(), m_score, getLeftTimestamp(), getRightTimestamp());
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		BinaryAssociation other = (BinaryAssociation)obj;
		
		return getLeftTrackletId().equals(other.getLeftTrackletId())
				&& getRightTrackletId().equals(other.getRightTrackletId())
				&& Double.compare(m_score, other.m_score) == 0
				&& getLeftTimestamp() == other.getLeftTimestamp()
				&& getRightTimestamp() == other.getRightTimestamp();
	}
}
