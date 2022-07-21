package jarvey.streams.turn;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import utils.stream.FStream;

import jarvey.streams.zone.ZoneLineRelationEvent;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZoneSequence {
	@SerializedName("node") private String m_nodeId;
	@SerializedName("luid") private long m_luid;
	@SerializedName("visits") private List<ZoneTravel> m_visits;
	
	public static ZoneSequence from(ZoneLineRelationEvent ev) {
		return new ZoneSequence(ev.getNodeId(), ev.getLuid(), Lists.newArrayList(ZoneTravel.open(ev)));
	}
	
	public static ZoneSequence from(String nodeId, long luid, ZoneTravel first) {
		return new ZoneSequence(nodeId, luid, Lists.newArrayList(first));
	}
	
	private ZoneSequence(String nodeId, long luid, List<ZoneTravel> travels) {
		m_nodeId = nodeId;
		m_luid = luid;
		m_visits = travels;
	}
	
	public String getNodeId() {
		return m_nodeId;
	}
	
	public long getLuid() {
		return m_luid;
	}
	
	public int getVisitCount() {
		return m_visits.size();
	}
	
	public ZoneTravel getVisit(int index) {
		return m_visits.get(index);
	}
	
	public List<ZoneTravel> getVisitAll() {
		return m_visits;
	}
	
	public ZoneTravel getLastZoneTravel() {
		return Iterables.getLast(m_visits);
	}
	
	public List<String> getZoneIdSequence() {
		return FStream.from(m_visits).map(ZoneTravel::getZoneId).toList();
	}
	
	public void append(ZoneTravel travel) {
		m_visits.add(travel);
	}
	
	@Override
	public String toString() {
		String seqStr =  FStream.from(m_visits)
								.map(trv -> String.format("%s%s", trv.getZoneId(), trv.isClosed() ? "" : "?"))
								.join('-');
		return String.format("%s/%d: %s", m_nodeId, m_luid, seqStr);
	}
}