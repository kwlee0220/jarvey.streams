package jarvey.streams.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import utils.CSV;
import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.streams.BinaryAssociationCollection;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationClosure implements Association {
	@SerializedName("id") private final String m_id;
	@SerializedName("tracklets") private final Set<TrackletId> m_trackletIds;
	@SerializedName("supports") private final List<BinaryAssociation> m_supports;
	@SerializedName("score") private double m_score;
	@SerializedName("first_ts") private long m_firstTs;
	@SerializedName("ts") private long m_ts;
	
	public static AssociationClosure from(Iterable<BinaryAssociation> supports) {
		return new AssociationClosure(supports);
	}
	
	public static AssociationClosure singleton(TrackletId trkId, long ts) {
		return new AssociationClosure(trkId, ts);
	}
	
	private AssociationClosure(Iterable<BinaryAssociation> supports) {
		long maxTs = 0;
		long minTs = Long.MAX_VALUE;
		BinaryAssociation leader = null;
		double totalScore = 0;
		int count = 0;
		m_trackletIds = Sets.newHashSet();
		for ( BinaryAssociation ba: supports ) {
			totalScore += ba.getScore();
			if ( ba.getTimestamp() > maxTs ) {
				maxTs = ba.getTimestamp();
			}
			if ( ba.getFirstTimestamp() < minTs ) {
				minTs = ba.getFirstTimestamp();
				leader = ba;
			}
			else if ( ba.getFirstTimestamp() == minTs && leader.getId().compareTo(ba.getId()) > 0 ) {
				minTs = ba.getFirstTimestamp();
				leader = ba;
			}

			m_trackletIds.add(ba.getLeftTrackletId());
			m_trackletIds.add(ba.getRightTrackletId());
			++count;
		}
		
		m_id = leader.getId();
		m_supports = Lists.newArrayList(supports);
		m_score = totalScore / count;
		m_firstTs = minTs;
		m_ts = maxTs;
	}
	
	private AssociationClosure(TrackletId trkId, long ts) {
		m_id = trkId.toString();
		m_trackletIds = Collections.singleton(trkId);
		m_supports = Collections.emptyList();
		m_score = 0;
		m_firstTs = ts;
		m_ts = ts;
	}
	
	@Override
	public String getId() {
		return m_id;
	}

	@Override
	public Set<TrackletId> getTracklets() {
		return m_trackletIds;
	}

	@Override
	public double getScore() {
		return m_score;
	}

	@Override
	public long getFirstTimestamp() {
		return m_firstTs;
	}

	@Override
	public long getTimestamp() {
		return m_ts;
	}
	
	public List<BinaryAssociation> getSupports() {
		return m_supports;
	}
	
	public static enum ExtendType {
		UNCHANGED,
		UPDATED,
		EXTENDED,
		CREATED,
	};
	public static class Expansion {
		private final ExtendType m_type;
		private final AssociationClosure m_closure;
		
		static Expansion unchanged(AssociationClosure closure) {
			return new Expansion(ExtendType.UNCHANGED, closure);
		}
		static Expansion updated(AssociationClosure closure) {
			return new Expansion(ExtendType.UPDATED, closure);
		}
		static Expansion extended(AssociationClosure closure) {
			return new Expansion(ExtendType.EXTENDED, closure);
		}
		static Expansion created(AssociationClosure closure) {
			return new Expansion(ExtendType.CREATED, closure);
		}
		
		Expansion(ExtendType type, AssociationClosure closure) {
			m_type = type;
			m_closure = closure;
		}
		
		public ExtendType type() {
			return m_type;
		}
		
		public AssociationClosure association() {
			return m_closure;
		}
		
		@Override
		public String toString() {
			return String.format("[%s] %s", m_type.name(), m_closure);
		}
	}

	@Override
	public AssociationClosure removeTracklet(TrackletId trkId) {
		if ( getTracklets().contains(trkId) ) {
			// 주어진 node에 관련된 tracklet이 포함되지 않는 association만을
			// 뽑아서 새로 closure를 생성한다.
			List<BinaryAssociation> newSupports = Funcs.filter(m_supports, ba -> !ba.containsTracklet(trkId));
			if ( newSupports.size() > 0 ) {
				return new AssociationClosure(newSupports);
			}
			else {
				return null;
			}
		}
		else {
			return this;
		}
	}
	
	public AssociationClosure merge(AssociationClosure other) {
		Set<BinaryAssociation> mergeds = Sets.newHashSet();
		mergeds.addAll(getSupports());
		mergeds.addAll(other.getSupports());
		return AssociationClosure.from(mergeds);
	}
	
	public AssociationClosure resolveConflict(Association conflict) {
		List<BinaryAssociation> newSupports
			= Funcs.filter(m_supports, ba -> !Collections.disjoint(ba.getTracklets(), conflict.getTracklets()));
		if ( newSupports.size() >= 2 ) {
			return AssociationClosure.from(newSupports);
		}
		else {
			return null;
		}
	}
	
	public AssociationClosure mergeWithoutConflicts(AssociationClosure conflict, boolean ignoreIdentity) {
		Set<TrackletId> leftDiffs = Sets.difference(conflict.getTracklets(), getTracklets());
		Set<TrackletId> conflictingTrkIds = Funcs.filter(leftDiffs, tid -> containsNode(tid.getNodeId()));
		
		BinaryAssociationCollection supports = new BinaryAssociationCollection(false); 
		FStream.from(conflict.getSupports())
				.filter(ba -> !ba.intersectsTracklet(conflictingTrkIds))
				.forEach(supports::add);
		if ( supports.size() == 0 && ignoreIdentity ) {
			return null;
		}
		else {
			m_supports.forEach(supports::add);
			return AssociationClosure.from(supports);
		}
	}
	
	public static List<TrackletId> parseAssociationString(String str) {
		return CSV.parseCsv(str, '-')
					.map(TrackletId::fromString)
					.toList();
	}
	
	public static String toString(TrackletId leaderId, List<TrackletId> trkIds) {
		return FStream.from(trkIds)
						.map(trkId -> {
							if ( trkId.equals(leaderId) ) {
								return String.format("%s[*%s]", trkId.getNodeId(), trkId.getTrackId());
							}
							else {
								return trkId.toString();
							}
						})
						.join('-');
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_supports);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		AssociationClosure other = (AssociationClosure)obj;
		return m_supports.equals(other.m_supports);
	}
	
	@Override
	public String toString() {
		String assocStr = FStream.from(m_trackletIds)
								.map(TrackletId::toString)
								.sort()
								.map(id -> id.equals(m_id) ? toLeaderString(id) : id)
								.join('-');
		return String.format("%s:%.2f#%d (%d)", assocStr, m_score, m_ts, m_firstTs);
	}
	
	private String toLeaderString(String id) {
		TrackletId trkId = TrackletId.fromString(id);
		return new TrackletId(trkId.getNodeId(), "*"+trkId.getTrackId()).toString();
	}
}
