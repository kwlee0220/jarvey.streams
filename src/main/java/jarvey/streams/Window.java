package jarvey.streams;

import com.google.common.base.Objects;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Window implements Comparable<Window> {
	private final long m_beginTime;
	private final long m_endTime;
	private boolean m_closed = false;
	
	public Window(long beginTs, long sizeMillis) {
		m_beginTime = beginTs;
		m_endTime = beginTs + sizeMillis;
	}
	
	public void close() {
		if ( !m_closed ) {
			m_closed = true;
		}
	}
	
	public boolean isClosed() {
		return m_closed;
	}
	
	public boolean isBefore(long ts) {
		return ts >= m_endTime;
	}
	
	public boolean isAfter(long ts) {
		return ts < m_beginTime;
	}
	
	public boolean contains(long ts) {
		return ts >= m_beginTime && ts < m_endTime;
	}
	
	public long beginTime() {
		return m_beginTime;
	}
	
	public long endTime() {
		return m_endTime;
	}

	@Override
	public int compareTo(Window o) {
		return Long.compare(m_beginTime, o.m_beginTime);
	}
	
	public int compareTo(long ts) {
		if ( ts < m_beginTime ) {
			return 1;
		}
		else if ( ts >= m_endTime ) {
			return -1;
		}
		else {
			return 0;
		}
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(m_beginTime, m_endTime);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null  || obj.getClass() != getClass() ) {
			return false;
		}
		
		Window other = (Window)obj;
		return Objects.equal(m_beginTime, other.m_beginTime)
				&& Objects.equal(m_endTime, other.m_endTime);
	}
	
	@Override
	public String toString() {
//		String stateStr = m_closed ? "C" : "O";
		return String.format("[%d:%d)", m_beginTime, m_endTime);
	}
}