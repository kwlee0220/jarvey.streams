package jarvey.streams.assoc;

import java.util.Collection;
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

import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Association implements Timestamped {
	@SerializedName("id") private final String m_id;
	@SerializedName("tracklets") private final Set<TrackletId> m_trackletIds;
	@SerializedName("supports") private final List<BinaryAssociation> m_supports;
	@SerializedName("score") private double m_score;
	@SerializedName("first_ts") private long m_firstTs;
	@SerializedName("ts") private long m_ts;
	
	public static Association from(Iterable<BinaryAssociation> supports) {
		return new Association(supports);
	}
	
	public static Association singleton(TrackletId trkId, long ts) {
		return new Association(trkId, ts);
	}
	
	private Association(Iterable<BinaryAssociation> supports) {
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
	
	private Association(TrackletId trkId, long ts) {
		m_id = trkId.toString();
		m_trackletIds = Collections.singleton(trkId);
		m_supports = Collections.emptyList();
		m_score = 0;
		m_firstTs = ts;
		m_ts = ts;
	}
	
	public String getId() {
		return m_id;
	}

	public Set<TrackletId> getTracklets() {
		return m_trackletIds;
	}
	
	/**
	 * 주어진 node에서 참여한 tracklet의 id를 반환한다.
	 *
	 * @param nodeId	노드 식별자.
	 * @return		해당 노드에서 참여하는 tracklet의 식별자.
	 * 				만일 주어진 노드에 해당하는 tracklet이 없는 경우는 {@code null}를 반환함.
	 */
	public TrackletId getTrackletId(String nodeId) {
		return Funcs.findFirst(getTracklets(), t -> t.getNodeId().equals(nodeId)).getOrNull();
	}

	public double getScore() {
		return m_score;
	}

	public long getFirstTimestamp() {
		return m_firstTs;
	}
	
	public void setFirstTimestamp(long firstTs) {
		m_firstTs = firstTs;
	}

	public long getTimestamp() {
		return m_ts;
	}
	
	/**
	 * Association에 참여하는 tracklet의 갯수를 반환한다.
	 *
	 * @return	참여 tracklet 갯수.
	 */
	public int size() {
		return getTracklets().size();
	}
	
	/**
	 * Association에 참여하는 모든 노드들의 식별자를 반환한다.
	 *
	 * @return	노드 식별자 집합.
	 */
	public Set<String> getNodes() {
		return Funcs.map(getTracklets(), TrackletId::getNodeId);
	}
	
	public String getTrackletsString() {
		return FStream.from(m_trackletIds)
						.map(TrackletId::toString)
						.sort()
						.map(id -> id.equals(m_id) ? toLeaderString(id) : id)
						.join('-');
	}
	
	/**
	 * 주어진 tracklet이 본 association에 포함되는지 여부를 반환한다.
	 * 
	 * @param trackletId	{@link TrackletId} 객체.
	 * @return	주어진 {@code trackletId}가 포함된 경우는 {@code true}, 그렇지 않은 경우는 {@code false}.
	 */
	public boolean containsTracklet(TrackletId trackletId) {
		return Funcs.exists(getTracklets(), t -> t.equals(trackletId));
	}
	
	/**
	 * 주어진 node에 생성된 tracklet이 본 association이 포함되었는지 여부를 반환한다.
	 * 
	 * @param nodeId	노드 식별자.
	 * @return	주어진 {@code nodeId}가 포함된 경우는 {@code true}, 그렇지 않은 경우는 {@code false}.
	 */
	public boolean containsNode(String nodeId) {
		return Funcs.exists(getTracklets(), t -> t.getNodeId().equals(nodeId));
	}
	
	public boolean intersectsTracklet(Association assoc) {
		return Funcs.intersects(getTracklets(), assoc.getTracklets());
	}
	
	public boolean intersectsTracklet(Collection<TrackletId> trkIds) {
		return Funcs.intersects(getTracklets(), trkIds);
	}
	
	public boolean intersectsNode(Association assoc) {
		return Funcs.intersects(getNodes(), assoc.getNodes());
	}
	
	public Set<TrackletId> intersectionTracklet(Association assoc) {
		return Sets.intersection(getTracklets(), assoc.getTracklets());
	}
	
	/**
	 * 본 association을 구성하는 tracklet들과 주어진 key와 동일 여부를 반환한다.
	 * 
	 * @param key	tracklet id 집합.
	 * @return	key와 동일한 tracklet으로 구성된 경우에는 {@code true},
	 * 			그렇지 않은 경우에는 {@code false}.
	 */
	public boolean match(Set<TrackletId> key) {
		return getTracklets().equals(key);
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
		private final Association m_closure;
		
		static Expansion unchanged(Association closure) {
			return new Expansion(ExtendType.UNCHANGED, closure);
		}
		static Expansion updated(Association closure) {
			return new Expansion(ExtendType.UPDATED, closure);
		}
		static Expansion extended(Association closure) {
			return new Expansion(ExtendType.EXTENDED, closure);
		}
		static Expansion created(Association closure) {
			return new Expansion(ExtendType.CREATED, closure);
		}
		
		Expansion(ExtendType type, Association closure) {
			m_type = type;
			m_closure = closure;
		}
		
		public ExtendType type() {
			return m_type;
		}
		
		public Association association() {
			return m_closure;
		}
		
		@Override
		public String toString() {
			return String.format("[%s] %s", m_type.name(), m_closure);
		}
	}

	public Association removeTracklet(TrackletId trkId) {
		if ( getTracklets().contains(trkId) ) {
			// 주어진 node에 관련된 tracklet이 포함되지 않는 association만을
			// 뽑아서 새로 closure를 생성한다.
			List<BinaryAssociation> newSupports = Funcs.filter(m_supports, ba -> !ba.containsTracklet(trkId));
			if ( newSupports.size() > 0 ) {
				return new Association(newSupports);
			}
			else {
				return null;
			}
		}
		else {
			return this;
		}
	}
	
	/**
	 * 두 association 사이의 관계를 반환한다.
	 * 두 association 사이의 관계를 아래와 같다.
	 * <dl>
	 * 	<dt>{@link BinaryRelation#DISJOINT}:</dt>
	 * 	<dd>두 association을 구성하는 tracklet id들 사이에 교집합이 없는 경우.</dd> 
	 * 	<dt>{@link BinaryRelation#MERGEABLE}:</dt>
	 * 	<dd>두 association을 구성하는 tracklet id들 사이에 교집합을 제외한 tracklet id 간에
	 * 		동일 node의 다른 tracklet id가 존재하지 않는 경우.</dd> 
	 * 	<dt>{@link BinaryRelation#CONFLICT}:</dt>
	 * 	<dd>두 association을 구성하는 tracklet id들 사이에 교집합을 제외한 tracklet id 간에
	 * 		동일 node의 다른 tracklet id가 존재하는 경우.</dd> 
	 * </dl>
	 * 
	 * @param other
	 * @return
	 */
	public BinaryRelation relate(Association other) {
		Set<TrackletId> overlap = Sets.intersection(getTracklets(), other.getTracklets());
		if ( overlap.isEmpty() ) {
			return BinaryRelation.DISJOINT;
		}

		Set<TrackletId> leftTrkIdDiffs = Sets.difference(getTracklets(), overlap);
		Set<TrackletId> rightTrkIdDiffs = Sets.difference(other.getTracklets(), overlap);
		
		if ( leftTrkIdDiffs.isEmpty() && rightTrkIdDiffs.isEmpty() ) {
			return BinaryRelation.SAME;
		}
		
		if ( rightTrkIdDiffs.isEmpty() ) {
			return BinaryRelation.LEFT_SUBSUME;
		}
		
		if ( leftTrkIdDiffs.isEmpty() ) {
			return BinaryRelation.RIGHT_SUBSUME;
		}
		
		Set<String> leftNodeDiffs = Funcs.map(leftTrkIdDiffs, TrackletId::getNodeId);
		Set<String> rightNodeDiffs = Funcs.map(rightTrkIdDiffs, TrackletId::getNodeId);
		if ( Funcs.intersects(leftNodeDiffs, rightNodeDiffs) ) {
			return BinaryRelation.CONFLICT;
		}
		else {
			return BinaryRelation.MERGEABLE;
		}
	}
	
	/**
	 * 두 association이 서로 conflict 여부를 반환한다.
	 *
	 * @param other		비교 대상 association
	 * @return		Conflict인 경우는 true 그렇지 않은 경우는 false
	 */
	public boolean isConflict(Association other) {
		return relate(other) == BinaryRelation.CONFLICT;
	}
	
	/**
	 * 두 association 사이의 우월성 여부를 반환한다.
	 * 
	 * Association A가 association B보다 다음과 같은 경우에 우월하다고 정의한다.
	 * <ul>
	 * 	<li> A가 B가 conflict한 경우에는 A가 B보다 score가 높다.
	 * 	<li> A와 B가 compatible한 경우에는 A가 B보다 specific하다.
	 * </ul>
	 * 
	 * @param other		대상 association.
	 * @return
	 */
	public boolean isSuperior(Association other) {
		switch ( relate(other) ) {
			case SAME:
				return getScore() > other.getScore();
			case DISJOINT:
				return false;
			case LEFT_SUBSUME:
				return true;
			case RIGHT_SUBSUME:
				return false;
			case CONFLICT:
				if ( size() < other.size() ) {
					return false;
				}
				else {
					return getScore() > other.getScore();
				}
			case MERGEABLE:
				return isMoreSpecific(other);
			default:
				throw new AssertionError();
		}
	}
	
	/**
	 * 본 association이 주어진 assoc보다 많은 정보가 포함되었는지 여부를 반환한다.
	 *
	 * @param assoc		대상 association 객체.
	 * @return		구체화 여부.
	 */
	public boolean isMoreSpecific(Association assoc) {
		// 본 association을 구성하는 tracklet의 수가 'assoc'의 tracklet 수보다 작다면
		// 'more-specific'일 수 없기 때문에 'false'를 반환한다.
		if ( size() < assoc.size() ) {
			return false;
		}

		// 'assoc'에만 포함된 tracklet이 존재하면 more-specific하다고 할 수 없기 때문에
		// false를 반환한다.
		if ( !getTracklets().containsAll(assoc.getTracklets()) ) {
			return false;
		}
		
		//
		// 본 association tracklet 집합이 assoc의 tracklet 집합과 동일하던지 superset 이다.
		//
		if ( size() > assoc.size() ) {
			// 참여된 tracklet 갯수가 더 많으면 더이상 확인할 필요없이 specific하다.
			return true;
		}
		else {
			// this와 assoc은 서로 동일한 tracklet으로 구성된 closure인 경우
			return getScore() > assoc.getScore();
		}
	}
	
	/**
	 * 두 association이 서로 disjoint 여부를 반환한다.
	 * 두 association 사이의 동일 tracklet를 포함하지 않는 경우를 disjoint라고 정의함.
	 *
	 * @param other		비교 대상 association
	 * @return		disjoin 여부.
	 */
	public boolean isDisjoint(Association other) {
		return Collections.disjoint(getTracklets(), other.getTracklets());
	}
	
	public boolean isInferior(Association other) {
		switch ( relate(other) ) {
			case SAME:
				return getScore() < other.getScore();
			case DISJOINT:
				return false;
			case LEFT_SUBSUME:
				return false;
			case RIGHT_SUBSUME:
				return true;
			case CONFLICT:
				if ( size() > other.size() ) {
					return false;
				}
				else {
					return getScore() < other.getScore();
				}
			case MERGEABLE:
				return other.isMoreSpecific(this);
			default:
				throw new AssertionError();
		}
	}

	/**
	 * 두 association이 서로 compatible 여부를 반환한다.
	 * 두 association 사이에 최소 1개 이상의 동일 node의 track이 존재하고,
	 * 모든 동일 node의 track 식별자가 동일한 경우 compatible함.
	 *
	 * @param other		비교 대상 association
	 * @return		Compatible한 경우는 True 그렇지 않은 경우는 False.
	 */
	public boolean isMergeable(Association other) {
		return relate(other) == BinaryRelation.MERGEABLE;
	}
	
	public Association merge(Association other) {
		Set<BinaryAssociation> mergeds = Sets.newHashSet();
		mergeds.addAll(getSupports());
		mergeds.addAll(other.getSupports());
		return Association.from(mergeds);
	}
	
	public List<Association> resolveConflict(Association conflict) {
		// 주어진 'conflict'와 conflict가 발생하지 않는 support만 찾는다.
		List<BinaryAssociation> nonConflictingSupports
			= Funcs.filter(m_supports, ba -> Collections.disjoint(ba.getTracklets(), conflict.getTracklets()));
		
		// 찾은 support들의 연결정보를 활용하여 connected closure들을 구한다.
		List<List<BinaryAssociation>> connectedSupportsGroup
			= BinaryAssociation.splitIntoConnectedClosure(nonConflictingSupports);
		
		// 각 connected support closure별로 Association을 생성한다.
		return FStream.from(connectedSupportsGroup)
						.map(grp -> Association.from(nonConflictingSupports))
						.toList();
	}
	
	public Association mergeWithoutConflicts(Association conflict, boolean ignoreIdentity) {
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
			return Association.from(supports);
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
		
		Association other = (Association)obj;
		return m_supports.equals(other.m_supports);
	}
	
	@Override
	public String toString() {
		return String.format("%s:%.2f#%d (%d)", getTrackletsString(), m_score, m_ts, m_firstTs);
	}
	
	private String toLeaderString(String id) {
		TrackletId trkId = TrackletId.fromString(id);
		return new TrackletId(trkId.getNodeId(), "*"+trkId.getTrackId()).toString();
	}
}
