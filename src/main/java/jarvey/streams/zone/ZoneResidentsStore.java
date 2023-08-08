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
	 * @param zoneId	전역 zone 식별자.
	 * @return	대상 zone에 소속된 물체 리스트.
	 */
	public Residents getResidentsOfZone(String zoneId);
	
	/**
	 * 시스템의 모든 물체들의 zone과 소속 물체들의 pair 리스트를 반환한다.
	 *
	 * @return	모든 zone들의 물체 위치 리스트.
	 */
	public List<KeyValue<String,Residents>> getResidentsAll();
}
