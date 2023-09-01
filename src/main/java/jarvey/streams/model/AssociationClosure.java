package jarvey.streams.model;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import utils.CSV;
import utils.Indexed;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationClosure implements Association, Iterable<BinaryAssociation> {
	private final String m_id;
	private final Set<TrackletId> m_trackletIds;
	private final List<BinaryAssociation> m_supports;
	private double m_score;
	private long m_firstTs;
	private long m_ts;
	
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

			m_trackletIds.add(ba.getLeftTrackId());
			m_trackletIds.add(ba.getRightTrackId());
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
	
	public AssociationClosure duplicate() {
		return new AssociationClosure(m_supports);
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
	
	public boolean isSingleton() {
		return m_trackletIds.size() == 1;
	}
	
	@Override
	public Iterator<BinaryAssociation> iterator() {
		return m_supports.iterator();
	}
	
	public List<BinaryAssociation> getSupports() {
		return m_supports;
	}
	
	public List<BinaryAssociation> find(TrackletId trkId) {
		return Funcs.filter(m_supports, ba -> ba.containsTracklet(trkId));
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
	
	public Expansion expand(BinaryAssociation assoc, boolean allowAlternative) {
		Set<TrackletId> trackOverlap = Sets.intersection(getTracklets(), assoc.getTracklets());
		if ( trackOverlap.size() == 2 ) {
			// closure에 assoc을 구성하는 두 tracklet-id를 모두 포함한 경우.
			Indexed<BinaryAssociation> prev = Funcs.findFirstIndexed(m_supports, assoc::match);
			if ( prev != null ) {
				// 동일 구조의 binary association이 포함된 경우.
				if ( prev.value().getScore() < assoc.getScore() ) {
					List<BinaryAssociation> supports = Lists.newArrayList(m_supports);
					supports.set(prev.index(), assoc);
					AssociationClosure extended = AssociationClosure.from(supports);
					
					return Expansion.updated(extended);
				}
			}
			else {
				// 동일 구조의 binary association은 없지만, transitivity-rule로 포함되었던 경우.
				if ( getScore() < assoc.getScore() ) {
					AssociationClosure extended = AssociationClosure.from(Funcs.addCopy(m_supports, assoc));
					return Expansion.updated(extended);
				}
			}
			
			return Expansion.unchanged(this);
		}
		else if ( trackOverlap.size() == 1 ) {
			// closure에 assoc을 구성하는 두 tracklet-id 중 한 개만 포함한 경우.
			
			// 겹친 binary association 에서 본 closure에 포함되지 않은 tracklet을 뽑아서
			// 이 tracklet과 동일한 node에서 생성된 다른 tracklet을 포함한 binary association이
			// 있는가 조사한다. 
			TrackletId other = assoc.getOther(Funcs.getFirst(trackOverlap));
			Indexed<BinaryAssociation> conflict = Funcs.findFirstIndexed(m_supports,
															ba -> ba.getNodes().contains(other.getNodeId()));
			if ( conflict != null ) {
				// conflict가 유발되는 경우
				if ( allowAlternative ) {
					List<BinaryAssociation> newSupports = Lists.newArrayList(m_supports);
					Funcs.removeIf(newSupports, ba -> ba.getNodes().contains(other.getNodeId()));
					newSupports.add(assoc);
					AssociationClosure created = AssociationClosure.from(newSupports);
					
					return Expansion.created(created);
				}
				else {
					return Expansion.unchanged(this);
				}
			}
			else {
				List<BinaryAssociation> supports = Funcs.addCopy(m_supports, assoc);
				return Expansion.extended(AssociationClosure.from(supports));
			}
		}
		else {
			// closure에 assoc을 구성하는 tracklet-id를 모두 포함하지 않은 경우.
			return Expansion.unchanged(this);
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

	private static double calcScore(Iterable<BinaryAssociation> supports) {
		return FStream.from(supports)
						.mapToDouble(BinaryAssociation::getScore)
						.average()
						.getOrElse(0d);
	}
	
	private static long calcTimestamp(Iterable<BinaryAssociation> supports) {
		List<Long> maxTses = FStream.from(supports)
									.mapToLong(BinaryAssociation::getTimestamp)
									.maxMultiple();
		return maxTses.isEmpty() ? -1 : maxTses.get(0); 
	}
	
	private static Tuple<String,Long> calcFirstTimestamp(List<BinaryAssociation> supports) {
		int idx = Funcs.argmin(supports, ba -> ba.getFirstTimestamp());
		BinaryAssociation leader = supports.get(idx);
		return Tuple.of(leader.getId(), leader.getFirstTimestamp());
	}
	
	public DAO toDao() {
		DAO dao = new DAO();
		dao.m_id = m_id;
		dao.m_trackletIds = FStream.from(m_trackletIds).sort().toList();
		dao.m_score = m_score;
		dao.m_firstTs = m_firstTs;
		dao.m_ts = m_ts;
		
		return dao;
	}
	
	public static List<TrackletId> parseAssociationString(String str) {
		return CSV.parseCsv(str, '-')
					.map(TrackletId::fromString)
					.toList();
	}
	
	public static String toString(List<TrackletId> trkIds) {
		return FStream.from(trkIds).join('-');
	}
	
	public static class DAO implements Timestamped {
		@SerializedName("id") private String m_id;
		@SerializedName("tracklets") private List<TrackletId> m_trackletIds;
		@SerializedName("score") private double m_score;
		@SerializedName("first_ts") private long m_firstTs;
		@SerializedName("ts") private long m_ts;
		
		public String getId() {
			return m_id;
		}
		
		public List<TrackletId> getTrackletIds() {
			return m_trackletIds;
		}
		
		public double getScore() {
			return m_score;
		}
		
		public boolean isSingleton() {
			return m_trackletIds.size() == 1;
		}
		
		public long getFirstTimestamp() {
			return m_firstTs;
		}
		
		@Override
		public long getTimestamp() {
			return m_ts;
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

	
	public Record deactivate() {
		return new Record(Lists.newArrayList(m_supports));
	}
	public static final class Record {
		@SerializedName("supports") private List<BinaryAssociation> supports;
		
		private Record(List<BinaryAssociation> supports) {
			this.supports = supports;
		}
		
		public AssociationClosure activate() {
			return AssociationClosure.from(supports);
		}
		
		@Override
		public String toString() {
			String binding = FStream.from(supports)
									.flatMapIterable(BinaryAssociation::getTracklets)
									.distinct()
									.sort()
									.map(TrackletId::toString)
									.join('-');
			double score = calcScore(supports);
			long ts = calcTimestamp(supports);
			Tuple<String,Long> tup = calcFirstTimestamp(supports);
			return String.format("[%s] %s:%.2f#%d (%d)", tup._1, binding, score, ts, tup._2);
		}
	}
}
