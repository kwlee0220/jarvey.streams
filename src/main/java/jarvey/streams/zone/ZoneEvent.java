package jarvey.streams.zone;

import javax.annotation.Nullable;

import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.Timestamped;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZoneEvent implements Timestamped {
	@SerializedName("track") private String m_trackId;
	@SerializedName("relation") private ZoneLineRelation m_relation;
	@Nullable @SerializedName("zone") private String m_zoneId;
	@SerializedName("ts") private long m_ts;
	
	public ZoneEvent(String trackId, ZoneLineRelation relation, @Nullable String zoneId, long ts) {
		m_trackId = trackId;
		m_relation = relation;
		m_zoneId = zoneId;
		m_ts = ts;
	}
	
	public String getTrackId() {
		return m_trackId;
	}
	
	public ZoneLineRelation getRelation() {
		return m_relation;
	}
	
	public String getZoneId() {
		return m_zoneId;
	}
	
	public boolean isUnassigned() {
		return m_relation.equals(ZoneLineRelation.Unassigned);
	}
	
	public boolean isEntered() {
		return m_relation.equals(ZoneLineRelation.Entered);
	}
	
	public boolean isLeft() {
		return m_relation.equals(ZoneLineRelation.Left);
	}
	
	public boolean isInside() {
		return m_relation.equals(ZoneLineRelation.Inside);
	}
	
	public boolean isThrough() {
		return m_relation.equals(ZoneLineRelation.Through);
	}
	
	public boolean isDeleted() {
		return m_relation.equals(ZoneLineRelation.Deleted);
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	@Override
	public String toString() {
		return String.format("%s[id=%s, zone=%s, ts=%d]", m_relation, m_trackId, m_zoneId, m_ts);
	}
}