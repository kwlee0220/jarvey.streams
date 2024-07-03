package jarvey.streams.assoc;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import utils.Indexed;
import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryAssociationCollection implements Iterable<BinaryAssociation>  {
	private final List<BinaryAssociation> m_associations;
	private final boolean m_allowConflict;
	
	public BinaryAssociationCollection(boolean allowConflict) {
		this(Lists.newArrayList(), allowConflict);
	}
	
	public BinaryAssociationCollection(List<BinaryAssociation> associations, boolean allowConflict) {
		m_associations = associations;
		m_allowConflict = allowConflict;
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
	public Iterator<BinaryAssociation> iterator() {
		return m_associations.iterator();
	}

	/**
	 * 주어진 tracklet을 포함하는 모든 association을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet의 식별자.
	 * @return	검색된 association의 stream 객체.
	 */
	public List<BinaryAssociation> find(TrackletId trkId) {
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
	public BinaryAssociation get(Set<TrackletId> key) {
		return Funcs.findFirst(m_associations, a -> key.equals(a.getTracklets())).getOrNull();
	}

	/**
	 * 주어진 tracklet 들로 구성된 association 객체와 collection 내의 순번을 반환한다.
	 *
	 * @param key	검색 키로 사용할 tracklet 집합.
	 * @return	검색된 association 및 순번 객체. 존재하지 않는 경우에는 null.
	 */
	public Indexed<BinaryAssociation> getIndexed(Set<TrackletId> key) {
		return FStream.from(m_associations)
						.zipWithIndex()
						.findFirst(t -> key.equals(t._1.getTracklets()))
						.map(t -> Indexed.with(t._1, t._2))
						.getOrNull();
	}
	
	public boolean update(BinaryAssociation assoc) {
		BinaryAssociation prev = Funcs.replaceFirst(m_associations, ba -> ba.match(assoc), assoc);
		if ( prev != null ) {
			return true;
		}
		else {
			return add(assoc);
		}
	}
	
	public BinaryAssociation remove(TrackletId trkId1, TrackletId trkId2) {
		Set<TrackletId> trkIds = Sets.newHashSet(trkId1, trkId2);
		return Funcs.removeFirstIf(m_associations, ba -> ba.getTracklets().equals(trkIds));
	}
	
	/**
	 * 주어진 tracklet들로 구성된 association을 collection에서 제거한다.
	 *
	 * @param key	tracklet id 집합
	 * @return	제거된 association 객체. 해당 키의 association 존재하지 않은 경우는 {@code null}.
	 */
	public List<BinaryAssociation> removeAll(Collection<TrackletId> key) {
		return Funcs.removeIf(m_associations, ba -> ba.intersectsTracklet(key));
	}
	
	public boolean add(BinaryAssociation assoc) {
		return m_allowConflict ? addAllowConflict(assoc) : addDisallowConflict(assoc);
	}

	public static List<BinaryAssociation> selectBestAssociations(List<BinaryAssociation> assocList) {
		List<BinaryAssociation> sorteds = FStream.from(assocList)
												.sort(BinaryAssociation::getScore, true)
												.toList();
		List<BinaryAssociation> bestAssocList = Lists.newArrayList();
		while ( sorteds.size() > 0 ) {
			BinaryAssociation best = sorteds.remove(0);
			sorteds = Funcs.removeIf(sorteds, ba -> ba.intersectsTracklet(best));
		}
		
		return bestAssocList;
	}
	
	private boolean addAllowConflict(BinaryAssociation assoc) {
		// collection이 빈 경우는 바로 삽입하고 반환한다.
		if ( size() == 0 ) {
			m_associations.add(assoc);
			return true;
		}
		
		Set<TrackletId> trkIds = assoc.getTracklets();
		Indexed<BinaryAssociation> found = Funcs.findFirstIndexed(m_associations,
																ba -> ba.getTracklets().containsAll(trkIds))
												.getOrNull();
		if ( found != null ) {
			if ( found.value().getScore() < assoc.getScore() ) {
				m_associations.set(found.index(), assoc);
				return true;
			}
			else {
				return false;
			}
		}
		else {
			m_associations.add(assoc);
			return true;
		}
	}
	
	private boolean addDisallowConflict(BinaryAssociation assoc) {
		// collection이 빈 경우는 바로 삽입하고 반환한다.
		if ( size() == 0 ) {
			m_associations.add(assoc);
			return true;
		}
		
		Iterator<BinaryAssociation> iter = m_associations.iterator();
		while ( iter.hasNext() ) {
			BinaryAssociation ba = iter.next();
			
			switch ( ba.relate(assoc) ) {
				case SAME:
					if ( ba.getScore() < assoc.getScore() ) {
						iter.remove();
						m_associations.add(assoc);
						return true;
					}
					else {
						return false;
					}
				case CONFLICT:
					if ( ba.getScore() < assoc.getScore() ) {
						iter.remove();
					}
					break;
				default:
					break;
			}
		}
		m_associations.add(assoc);
		return true;
	}
	
	public void clear() {
		m_associations.clear();
	}
	
	@Override
	public String toString() {
		return m_associations.toString();
	}
}
