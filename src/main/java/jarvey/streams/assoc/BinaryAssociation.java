package jarvey.streams.assoc;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import utils.func.Funcs;

import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class BinaryAssociation {
	@SerializedName("id") private String m_id;
	@SerializedName("left") private TimedTracklet m_left;
	@SerializedName("right") private TimedTracklet m_right;
	@SerializedName("first_ts") private long m_firstTs;
	@SerializedName("score") private double m_score;
	
	public static class TimedTracklet implements Timestamped {
		@SerializedName("tracklet") private TrackletId m_tracklet;
		@SerializedName("ts") private long m_ts;
		
		TimedTracklet(TrackletId trkId, long ts) {
			m_tracklet = trkId;
			m_ts = ts;
		}
		
		public TrackletId getTrackletId() {
			return m_tracklet;
		}

		@Override
		public long getTimestamp() {
			return m_ts;
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

	public String getId() {
		return m_id;
	}
	
	public TrackletId getLeader() {
		return TrackletId.fromString(m_id);
	}
	
	public TimedTracklet getFollower() {
		if ( m_left.m_tracklet.toString().equals(m_id) ) {
			return m_right;
		}
		else {
			return m_left;
		}
	}

	public Set<TrackletId> getTracklets() {
		return Sets.newHashSet(m_left.m_tracklet, m_right.m_tracklet);
	}
	
	public List<TimedTracklet> getTimedTracklets() {
		return Arrays.asList(m_left, m_right);
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
	
	public boolean containsTracklet(TrackletId trackletId) {
		return m_left.m_tracklet.equals(trackletId) || m_right.m_tracklet.equals(trackletId);
	}
	
	public boolean intersectsTracklet(BinaryAssociation assoc) {
		return intersectsTracklet(assoc.getTracklets());
	}
	
	public boolean intersectsTracklet(Collection<TrackletId> trkIds) {
		return trkIds.contains(m_left.m_tracklet) || trkIds.contains(m_right.m_tracklet);
	}

	public double getScore() {
		return m_score;
	}

	public long getFirstTimestamp() {
		return m_firstTs;
	}
	
	public long getLeftTimestamp() {
		return m_left.m_ts;
	}
	
	public long getRightTimestamp() {
		return m_right.m_ts;
	}
	
	public long getTimestamp() {
		return Math.max(getLeftTimestamp(), getRightTimestamp());
	}

	public boolean match(BinaryAssociation other) {
		return Objects.equals(getLeftTrackletId(), other.getLeftTrackletId())
				&& Objects.equals(getRightTrackletId(), other.getRightTrackletId());
	}
	
	public BinaryRelation relate(BinaryAssociation other) {
		Set<TrackletId> otherTrkIds = other.getTracklets();
		
		boolean matchLeft = otherTrkIds.contains(m_left.m_tracklet);
		boolean matchRight = otherTrkIds.contains(m_right.m_tracklet);
		if ( !matchLeft && !matchRight ) {
			return BinaryRelation.DISJOINT;
		}
		else if ( matchLeft && matchRight ) {
			return BinaryRelation.SAME;
		}
		else {
			Set<String> otherNodeIds = Funcs.map(otherTrkIds, TrackletId::getNodeId);
			
			// match되지 않는 tracklet id를 사용하여 판단
			TrackletId unmatchedTrkId = matchLeft ? m_right.m_tracklet : m_left.m_tracklet;
			if ( otherNodeIds.contains(unmatchedTrkId.getNodeId()) ) {
				return BinaryRelation.CONFLICT;
			}
			else {
				return BinaryRelation.MERGEABLE;
			}
		}
	}
	
	@Override
	public String toString() {
		TrackletId trkId = TrackletId.fromString(m_id);
		if ( getLeftTrackletId().equals(trkId) ) {
			TrackletId leader = getLeftTrackletId();
			return String.format("%s[*%s]-%s:%.3f#%d",
								leader.getNodeId(), leader.getTrackId(), getRightTrackletId(),
								m_score, getTimestamp());
		}
		else {
			TrackletId leader = getRightTrackletId();
			return String.format("%s-%s[*%s]:%.3f#%d",
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
	
	public static List<List<BinaryAssociation>> splitIntoConnectedClosure(Iterable<BinaryAssociation> assocList) {
		List<Set<TrackletId>> idClosures = Lists.newArrayList();
		for ( BinaryAssociation ba: assocList ) {
			// 왼쪽/오른쪽 tracklet id에 따른 closure를 찾아 이 두 closure를 서로 합친다.
			Set<TrackletId> left = Funcs.findFirst(idClosures, c -> c.contains(ba.getLeftTrackletId())).getOrNull();
			Set<TrackletId> right = Funcs.findFirst(idClosures, c -> c.contains(ba.getRightTrackletId())).getOrNull();
			if ( left != null && right == null ) {
				left.add(ba.getRightTrackletId());
			}
			else if ( left == null && right != null ) {
				right.add(ba.getLeftTrackletId());
			}
			else if ( left == null && right == null ) {
				idClosures.add(Sets.newHashSet(ba.getTracklets()));
			}
			else {
				left.addAll(right);
				idClosures.remove(right);
			}
		}
		
		List<List<BinaryAssociation>> closures = Lists.newArrayList();
		for ( Set<TrackletId> trkSet: idClosures ) {
			List<BinaryAssociation> members = Funcs.filter(assocList, ba -> ba.intersectsTracklet(trkSet)); 
			closures.add(members);
		}
		return closures;
	}
}
