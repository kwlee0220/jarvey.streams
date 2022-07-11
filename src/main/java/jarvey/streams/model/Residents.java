package jarvey.streams.model;

import java.util.Collections;
import java.util.Set;

import com.google.gson.annotations.SerializedName;

import utils.func.Funcs;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Residents {
	@SerializedName("luids") private final Set<Long> m_luids;
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	
	public Residents() {
		m_luids = Collections.emptySet();
		m_frameIndex = 0;
		m_ts = 0;
	}
	
	public Residents(Set<Long> residents, long fidx, long ts) {
		m_luids = residents;
		m_frameIndex = fidx;
		m_ts = ts;
	}
	
	public Set<Long> getLuids() {
		return m_luids;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public Residents update(ZoneLineCross ev) {
		switch ( ev.getState() ) {
			case ZoneLineCross.STATE_UNASSIGNED:
			case ZoneLineCross.STATE_INSIDE:
			case ZoneLineCross.STATE_THROUGH:
			case ZoneLineCross.STATE_DELETED:
				return new Residents(m_luids, ev.getFrameIndex(), ev.getTimestamp());
			case ZoneLineCross.STATE_ENTERED:
				return new Residents(Funcs.add(m_luids, ev.getLuid()), ev.getFrameIndex(), ev.getTimestamp());
			case ZoneLineCross.STATE_LEFT:
				return new Residents(Funcs.remove(m_luids, ev.getLuid()), ev.getFrameIndex(), ev.getTimestamp());
			default:
				throw new AssertionError(String.format("unexpected %s event state: state=%s",
													ev.getClass().getSimpleName(), ev.getState()));
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s[luids=%s, frame=%d, ts=%d]",
							getClass().getSimpleName(), m_luids, m_frameIndex, m_ts);
	}
}