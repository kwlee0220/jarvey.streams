package jarvey.streams.assoc;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import utils.Indexed;
import utils.Tuple;
import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationCollection implements Iterable<Association>  {
	private static final Logger s_logger = LoggerFactory.getLogger(AssociationCollection.class);
	
	private final String m_id;
	private final List<Association> m_associations;
	
	public AssociationCollection(String id) {
		m_id = id;
		m_associations = Lists.newArrayList();
	}
	
	public AssociationCollection(String id, List<Association> assocList) {
		m_id = id;
		m_associations = assocList;
	}
	
	public String getId() {
		return m_id;
	}
	
	/**
	 * Collection에 포함된 association 객체의 갯수를 반환한다.
	 *
	 * @return	association 객체의 갯수.
	 */
	public long size() {
		return m_associations.size();
	}

	@Override
	public Iterator<Association> iterator() {
		return m_associations.iterator();
	}
	
	public List<Association> find(TrackletId trkId) {
		return Funcs.filter(m_associations, a -> a.containsTracklet(trkId));
	}
	
	public boolean exists(TrackletId trkId) {
		return Funcs.exists(m_associations, a -> a.containsTracklet(trkId));
	}

	/**
	 * 주어진 tracklet 들로 구성된 association 객체를 검색한다.
	 *
	 * @param key	검색 키로 사용할 tracklet 집합.
	 * @return	검색된 association 객체. 존재하지 않는 경우에는 null.
	 */
	public Association get(Set<TrackletId> key) {
		return Funcs.findFirst(m_associations, a -> key.equals(a.getTracklets())).getOrNull();
	}

	/**
	 * 주어진 tracklet 들로 구성된 association 객체와 collection 내의 순번을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet 집합.
	 * @return	검색된 association 및 순번 객체. 존재하지 않는 경우에는 null.
	 */
	public Indexed<Association> getIndexed(Set<TrackletId> key) {
		return FStream.from(m_associations)
						.zipWithIndex()
						.findFirst(t -> key.equals(t.value().getTracklets()))
						.map(t -> Indexed.with(t.value(), t.index()))
						.getOrNull();
	}
	
	/**
	 * 주어진 tracklet을 포함하는 모든 association을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet의 식별자.
	 * @return	검색된 association의 stream 객체.
	 */
	public FStream<Association> findAll(TrackletId key) {
		return FStream.from(m_associations)
						.filter(a -> a.containsTracklet(key));
	}
	
	public FStream<Indexed<Association>> findIndexedAll(TrackletId key) {
		return FStream.from(m_associations)
						.zipWithIndex()
						.map(t -> Indexed.with(t.value(), t.index()))
						.filter(idxed -> idxed.value().containsTracklet(key));
	}
	
	public Association findSuperiorFirst(Association key) {
		return Funcs.findFirst(m_associations, cl -> cl.isSuperior(key)).getOrNull();
	}
	
	/**
	 * 주어진 tracklet들로 구성된 association을 collection에서 제거한다.
	 *
	 * @param key	tracklet id 집합
	 * @return	제거된 association 객체. 해당 키의 association 존재하지 않은 경우는 {@code null}.
	 */
	public Association remove(Set<TrackletId> key) {
		return Funcs.removeFirstIf(m_associations, a -> a.match(key));
	}
	
	public void removeIf(Predicate<? super Association> pred) {
		m_associations.removeIf(pred);
	}
	
	public Association remove(int index) {
		return m_associations.remove(index);
	}
	
	public List<Association> removeInferiors(Association key) {
		return Funcs.removeIf(m_associations, cl -> cl.isInferior(key));
	}
	
	public List<Association> add(Association assoc) {
		return add(assoc, true);
	}
	
	public List<Association> add(BinaryAssociation assoc) {
		return add(Association.from(Collections.singletonList(assoc)));
	}
	
	public List<Association> add(Association assoc, boolean expandOnConflict) {
		// collection이 빈 경우는 바로 삽입하고 반환한다.
		if ( size() == 0 ) {
			m_associations.add(assoc);
			return Collections.singletonList(assoc);
		}
		
		List<Association> updateds = Lists.newArrayList();
		Map<BinaryRelation,List<Association>> groups = Maps.newHashMap();
		Iterator<Association> iter = m_associations.iterator();
		while ( iter.hasNext() ) {
			Association current = iter.next();

			BinaryRelation rel = current.relate(assoc);
			if ( rel == BinaryRelation.SAME ) {
				if ( current.getScore() >= assoc.getScore() ) {
					return Collections.emptyList();
				}
				else {
					// 새로 삽입될 association이 동일 association이면서 점수가
					// 더 높은 경우는 replace시킨다.
					// 이때 기존 것의 firstTs가 더 작은 경우에는 이를 사용한다.
					if ( current.getFirstTimestamp() < assoc.getFirstTimestamp() ) {
						assoc.setFirstTimestamp(current.getFirstTimestamp());
					}
					iter.remove();
					
					m_associations.add(assoc);
					return Collections.singletonList(assoc);
				}
			}
			else if ( rel == BinaryRelation.LEFT_SUBSUME ) {
				// 이미 더 superior한 association이 존재하는 경우
				return Collections.emptyList();
			}
			else if ( rel == BinaryRelation.RIGHT_SUBSUME ) {
				// 기존 inferior한 association들을 모두 제거한다.
				iter.remove();
			}
			else {
				// 새로 삽입될 association과의 관계를 기준으로 grouping 시킨다.
				groups.computeIfAbsent(rel, k -> Lists.newArrayList()).add(current);
			}
		}
		
		boolean expanded = false;
		List<Association> mergeds = Collections.emptyList();
		
		List<Association> mergeables = groups.get(BinaryRelation.MERGEABLE);
		if ( mergeables != null && mergeables.size() > 0 ) {
			mergeds = Funcs.map(mergeables, m -> m.merge(assoc));
			List<Association> result = FStream.from(mergeds)
												.flatMap(m -> FStream.from(add(m, true)))
												.toList();
			if ( result.size() > 0 ) {
				updateds.addAll(result);
				expanded = true;
			}
		}

		List<Association> conflicts = groups.get(BinaryRelation.CONFLICT);
		if ( conflicts != null && conflicts.size() > 0 ) {
			if ( expandOnConflict ) {
				List<Association> resolveds = Lists.newArrayList();
				for ( Association conflict: conflicts ) {
					Association resolved = assoc.mergeWithoutConflicts(conflict, true);
					if ( resolved != null ) {
						resolveds.add(resolved);
						if ( s_logger.isDebugEnabled() ) {
							s_logger.debug("add conflict-resolved assoc={} -> {}, conflict={}",
											assoc.getTrackletsString(), resolved.getTrackletsString(),
											conflict.getTrackletsString());
						}
					}
				}
				List<Association> result = FStream.from(resolveds)
												.flatMap(r -> FStream.from(add(r, false)))
												.toList();
				if ( result.size() > 0 ) {
					updateds.addAll(result);
					expanded = true;
				}
			}
		}
		
		if ( !expanded ) {
			m_associations.add(assoc);
			updateds.add(assoc);
		}
		
		return updateds;
	}
	
	public AssociationCollection getBestAssociations(String id) {
		return new AssociationCollection(id, selectBestAssociations(m_associations));
	}
	
	public static List<Association> selectBestAssociations(List<Association> assocList) {
		List<Association> bestAssocList = Lists.newArrayList();
		
		// collection에 속한 모든 association들을 길이와 score 값을 기준을 정렬시킨다.
		List<Association> sorted = FStream.from(assocList)
											.sort(a -> Tuple.of(a.size(), a.getScore()), true)
											.toList();
		
		// 정렬된 association들을 차례대로 읽어 동일한 tracklet으로 구성된 inferior association들을
		// 삭제하는 방법으로 best association들을 구한다.
		while ( sorted.size() > 0 ) {
			Association best = sorted.remove(0);
			bestAssocList.add(best);
			
			// 선택된 association과 conflict를 발생하는 후보 association들을 conflict-resolve 시킨다.
			List<Association> compatibles = resolveConflict(best, sorted);
			
			// 정렬이 깨졌을 수 있기 때문에 다시 정렬시킨다.
			sorted = FStream.from(compatibles)
							.sort(a -> Tuple.of(a.size(), a.getScore()), true)
							.toList();
		}
		
		return bestAssocList;
	}
	
	public void clear() {
		m_associations.clear();
	}
	
	@Override
	public String toString() {
		return m_associations.toString();
	}
	
	public void resolveConflict(Association key) {
		List<Association> compatibles = resolveConflict(key, m_associations);
		m_associations.clear();
		for ( Association comp: compatibles ) {
			add(comp);
		}
	}
	
	public static List<Association> resolveConflict(Association key, List<Association> associations) {
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("resolve conflicts: key={}", key.getTrackletsString());
		}
		
		// 'm_associations'에 등록된 모든 association들을 closure와의 conflict 여부에 따라
		// 두가지 group으로 분류한다.
		Tuple<List<Association>, List<Association>> partioned
			= Funcs.partition(associations, key::intersectsTracklet);
		List<Association> conflicts = partioned._1;
		List<Association> compatibles = partioned._2;
		
		// Conflict group에 속한 각 association들을 conflict resolve한 결과를 conflict가
		// 발생되지 않는 association group에 추가하여 새로운 association group ('m_association')을 설정한다.
		while ( conflicts.size() > 0 ) {
			Association conflict = conflicts.remove(0);
			
			List<Association> resoloveds = conflict.resolveConflict(key);
			if ( resoloveds.size() > 0 ) {
				for ( Association resoloved: resoloveds ) {
					compatibles.add(resoloved);
					if ( s_logger.isDebugEnabled() ) {
						s_logger.debug("\treduces the conflicting one: conflict={}, reduced={}",
										key.getTrackletsString(), resoloved.getTrackletsString());
					}
				}
			}
			else {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("\tpurges the conflicting one: conflict={}", key.getTrackletsString());
				}
			}
		}
		
		return compatibles;
	}
}
