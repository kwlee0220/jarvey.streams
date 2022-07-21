package jarvey.streams.turn;

import com.google.gson.annotations.SerializedName;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ObjectTurn {
	@SerializedName("node") private String m_nodeId;
	@SerializedName("luid") private long m_luid;
	@SerializedName("turn") private String m_turn;
	
	public ObjectTurn(String nodeId, long luid, String turn) {
		m_nodeId = nodeId;
		m_luid = luid;
		m_turn = turn;
	}
	
	@Override
	public String toString() {
		return String.format("Turn[%s/%d: %s]", m_nodeId, m_luid, m_turn);
	}
}