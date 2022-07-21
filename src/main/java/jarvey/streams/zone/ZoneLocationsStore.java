/**
 * 
 */
package jarvey.streams.zone;

import java.util.List;

import utils.func.KeyValue;

import jarvey.streams.model.GUID;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ZoneLocationsStore {
	/**
	 * 주어진 전역 물체 식별자의 zone 위치 리스트를 반환한다.
	 *
	 * @param guid	전역 물체 식별자.
	 * @return	대상 물체가 위치한 zone 식별자 리스트.
	 */
	public ZoneLocations getZoneLocationsOfObject(GUID guid);
	
	/**
	 * 주어진 노드에 소속된 모든 물체들의 zone 위치 리스트를 반환한다.
	 *
	 * @param nodeId	노드 식별자.
	 * @return	노드에 소속된 모든 물체들의 zone 위치 리스트.
	 */
	public List<KeyValue<GUID,ZoneLocations>> getZoneLocationsOfNode(String nodeId);
	
	/**
	 * 시스템의 모든 물체들의 zone 위치 리스트를 반환한다.
	 *
	 * @return	모든 물체들의 zone 위치 리스트.
	 */
	public List<KeyValue<GUID,ZoneLocations>> getZoneLocationsAll();
}
