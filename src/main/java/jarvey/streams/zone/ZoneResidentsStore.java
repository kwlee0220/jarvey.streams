package jarvey.streams.zone;

import java.util.List;

import utils.func.KeyValue;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ZoneResidentsStore {
	/**
	 * 주어진 전역 zone에 소속된 물체 리스트를 반환한다.
	 *
	 * @param gzone	전역 zone 식별자.
	 * @return	대상 zone에 소속된 물체 리스트.
	 */
	public Residents getResidentsOfZone(GlobalZoneId gzone);
	
	/**
	 * 주어진 노드에 포함된 zone에 위치한 모든 물체 리스트를 반환한다.
	 *
	 * @param nodeId	노드 식별자.
	 * @return	노드에 소속된 모든 물체들의 zone 위치 리스트.
	 */
	public List<KeyValue<GlobalZoneId,Residents>> getResidentsOfNode(String nodeId);
	
	/**
	 * 시스템의 모든 물체들의 zone과 소속 물체들의 pair 리스트를 반환한다.
	 *
	 * @return	모든 zone들의 물체 위치 리스트.
	 */
	public List<KeyValue<GlobalZoneId,Residents>> getResidentsAll();
}
