package jarvey.streams.updatelog;

import jarvey.streams.model.Timestamped;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface KeyedUpdate extends Timestamped {
	/**
	 * 갱신 대상 키를 반환한다.
	 * 
	 * @return	키 값.
	 */
	public String getKey();
	
	/**
	 * 동일 키에 대한 마지막 갱신 여부를 반환한다.
	 * 
	 * @return	마지막 갱신인 경우는 {@code true}, 그렇지 않은 경우는 {@code false}.
	 */
	public boolean isLastUpdate();
}
