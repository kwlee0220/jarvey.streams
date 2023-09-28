package jarvey.streams.assoc;

import java.util.LinkedHashMap;
import java.util.Map;

import utils.func.FOption;

import jarvey.streams.model.TrackletId;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationCache extends LinkedHashMap<String,Association> {
	private static final long serialVersionUID = 1L;
	private static final Association ABSENT = Association.singleton(new TrackletId("", ""), -1);
	
	private final int m_maxSize;
	
	public AssociationCache(int maxSize) {
		super(maxSize, 0.75f, true);
		m_maxSize  = maxSize;
	}
	
	public FOption<Association> get(TrackletId trkId) {
		return get(trkId.toString());
	}
	
	public FOption<Association> get(String key) {
		Association assoc = super.get(key);
		if ( assoc == null ) {
			return FOption.empty();
		}
		else if ( assoc != ABSENT ) {
			return FOption.of(assoc);
		}
		else {
			return FOption.of(null);
		}
	}
	
	public void put(TrackletId trkId, Association assoc) {
		if ( assoc != null ) {
			assoc.getTracklets()
					.stream()
					.map(TrackletId::toString)
					.forEach(id -> super.put(id, assoc));
		}
		else {
			super.put(trkId.toString(), ABSENT);
		}
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<String, Association> eldest) {
		return size() > m_maxSize;
	}
}
