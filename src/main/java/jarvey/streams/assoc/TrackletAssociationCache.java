package jarvey.streams.assoc;

import java.util.LinkedHashMap;
import java.util.Map;

import utils.func.FOption;

import jarvey.streams.model.TrackletId;

import io.reactivex.rxjava3.core.Maybe;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TrackletAssociationCache extends LinkedHashMap<TrackletId,String> {
	private static final long serialVersionUID = 1L;
	
	private final int m_maxSize;
	
	public TrackletAssociationCache(int maxSize) {
		super(maxSize, 0.75f, true);
		m_maxSize  = maxSize;
	}
	
	public Maybe<FOption<String>> rxGet(TrackletId trkId) {
		FOption<String> oAssoc = get(trkId);
		return oAssoc.isPresent() ? Maybe.just(oAssoc) : Maybe.empty();
	}
	
	public FOption<String> get(TrackletId trkId) {
		String assocId = super.get(trkId);
		if ( assocId == null ) {
			return FOption.empty();
		}
		else {
			return FOption.of(assocId);
		}
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<TrackletId, String> eldest) {
		return size() > m_maxSize;
	}
}
