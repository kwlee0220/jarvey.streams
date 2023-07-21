package jarvey.streams.zone;

import java.util.Collections;
import java.util.Set;

import com.google.gson.annotations.SerializedName;

import utils.func.Funcs;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Residents {
	@SerializedName("luids") private final Set<String> m_trackIds;
	@SerializedName("frame_index") private final long m_frameIndex;
	@SerializedName("ts") private final long m_ts;
	
	public Residents() {
		m_trackIds = Collections.emptySet();
		m_frameIndex = 0;
		m_ts = 0;
	}
	
	public Residents(Set<String> residents, long fidx, long ts) {
		m_trackIds = residents;
		m_frameIndex = fidx;
		m_ts = ts;
	}
	
	public Set<String> getTrackIds() {
		return m_trackIds;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public Residents update(ZoneLineRelationEvent ev) {
		switch ( ev.getRelation() ) {
			case Deleted:
				if ( m_trackIds.size() > 0 ) {
					return new Residents(Collections.emptySet(), ev.getFrameIndex(), ev.getTimestamp());
				}
				else {
					return null;
				}
			case Entered:
			case Inside:
				if ( !m_trackIds.contains(ev.getTrackId()) ) {
					return new Residents(Funcs.add(m_trackIds, ev.getTrackId()), ev.getFrameIndex(), ev.getTimestamp());
				}
				else {
					return null;
				}
			case Left:
			case Unassigned:
			case Through:
				if ( m_trackIds.contains(ev.getTrackId()) ) {
					return new Residents(Funcs.remove(m_trackIds, ev.getTrackId()), ev.getFrameIndex(), ev.getTimestamp());
				}
				else {
					return null;
				}
			default:
				throw new AssertionError(String.format("unexpected %s event state: state=%s",
													ev.getClass().getSimpleName(), ev.getRelation()));
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[luids=%s, frame=%d, ts=%d]",
							getClass().getSimpleName(), m_trackIds, m_frameIndex, m_ts);
	}
}