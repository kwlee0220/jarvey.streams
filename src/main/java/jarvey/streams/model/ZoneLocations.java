package jarvey.streams.model;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import utils.func.Funcs;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZoneLocations {
	@SerializedName("zones") private Set<String> m_zoneIds = Sets.newHashSet();
	@SerializedName("frame_index") private long m_frameIndex;
	@SerializedName("ts") private long m_ts;
	@SerializedName("deleted") private boolean m_deleted = false;
	
	public ZoneLocations() { }
	public ZoneLocations(Set<String> zoneIds, long frameIndex, long ts, boolean deleted) {
		m_zoneIds = zoneIds;
		m_frameIndex = frameIndex;
		m_ts = ts;
		m_deleted = deleted;
	}
	
	public Set<String> getZoneIds() {
		return m_zoneIds;
	}
	
	public long getFrameIndex() {
		return m_frameIndex;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public boolean isDeleted() {
		return m_deleted;
	}
	
	public ZoneLocations update(ZoneLineCross ev) {
		switch ( ev.getState() ) {
			case ZoneLineCross.STATE_ENTERED:
				return new ZoneLocations(Funcs.add(m_zoneIds, ev.getZone()),
										ev.getFrameIndex(), ev.getTimestamp(), false);
			case ZoneLineCross.STATE_LEFT:
				return new ZoneLocations(Funcs.remove(m_zoneIds, ev.getZone()),
										ev.getFrameIndex(), ev.getTimestamp(), false);
			case ZoneLineCross.STATE_DELETED:
				return new ZoneLocations(m_zoneIds, ev.getFrameIndex(), ev.getTimestamp(), true);
			default:
				throw new AssertionError(String.format("unexpected %s event state: state=%s",
													ev.getClass().getSimpleName(), ev.getState()));
		}
	}
	
	@Override
	public String toString() {
		if ( !m_deleted ) {
			return String.format("%s[zones=%s, frame_idx=%d, ts=%d]",
								getClass().getSimpleName(),m_zoneIds, m_frameIndex, m_ts);
		}
		else {
			return String.format("Deleted[frame_idx=%d, ts=%d]", m_frameIndex, m_ts);
		}
	}
}