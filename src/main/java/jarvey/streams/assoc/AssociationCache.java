package jarvey.streams.assoc;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import jarvey.streams.model.TrackletId;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationCache {
	private final int m_maxSize;
	private final AssociationStore m_store;
	private final InternalMap m_assocMap;
	
	public AssociationCache(int maxSize, AssociationStore store) {
		m_maxSize  = maxSize;
		m_store  = store;
		m_assocMap = new InternalMap(maxSize);
	}
	
	public boolean contains(TrackletId trkId) {
		return m_assocMap.containsKey(trkId);
	}
	
	public Association get(TrackletId trkId) {
		return m_assocMap.get(trkId);
	}
	
	public Association getOrLoad(TrackletId trkId) {
		Association assoc = m_assocMap.get(trkId);
		if ( assoc == null ) {
			try {
				Association found = m_store.getAssociation(trkId);
				if ( found != null ) {
					// 동일 association을 등록한 tracklet에 대해서도
					// 이번에 load한 association이 보다 최신 값일 가능성이 있기 때문에
					// 새로 load된 association으로 갱신시킨다.
					found.getTracklets().forEach(t -> m_assocMap.putIfAbsent(t, found));
					assoc = found;
				}
			}
			catch ( SQLException e ) {
				throw new RuntimeException(e);
			}
		}
		return assoc;
	}
	
	public void put(TrackletId trkId, Association assoc) {
		m_assocMap.put(trkId, assoc);
	}
	
	public Set<TrackletId> getCachedAssociationKeys() {
		return m_assocMap.keySet();
	}
	
	private class InternalMap extends LinkedHashMap<TrackletId,Association> {
		private static final long serialVersionUID = 1L;

		InternalMap(int size) {
			super(size, 0.75f, true);
		}
		
		@Override
		protected boolean removeEldestEntry(Map.Entry<TrackletId, Association> eldest) {
			return size() > m_maxSize;
		}
	}
}
