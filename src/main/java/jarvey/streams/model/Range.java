package jarvey.streams.model;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;

import utils.func.Funcs;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class Range<T extends Comparable<T>> implements Comparable<Range<T>> {
	@SerializedName("min") private T m_min;
	@SerializedName("max") private T m_max;
	
	public static <T extends Comparable<T>> Range<T> between(T min, T max) {
		return new Range<>(min, max);
	}
	
	public static <T extends Comparable<T>> Range<T> all() {
		return new Range<>(null, null);
	}
	
	public static <T extends Comparable<T>> Range<T> atLeast(T min) {
		return new Range<>(min, (T)null);
	}
	
	public static <T extends Comparable<T>> Range<T> atMost(T max) {
		return new Range<>((T)null, max);
	}
	
	public static <T extends Comparable<T>> Range<T> at(T v) {
		return new Range<>(v, v);
	}
	
	private Range(T min, T max) {
		m_min = min;
		m_max = max;
	}
	
	public T min() {
		return m_min;
	}
	
	public T max() {
		return m_max;
	}
	
	public boolean isInfiniteMin() {
		return m_min == null;
	}
	
	public boolean isInfiniteMax() {
		return m_max == null;
	}
	
	public void set(Range<T> range) {
		m_min = range.m_min;
		m_max = range.m_max;
	}
	
	public void expand(T v) {
		m_min = min(m_min, v);
		m_max = max((m_max == null) ? m_min : m_max, v);
	}
	
	public boolean contains(T v) {
		if ( m_min == null && m_max == null) {
			return true;
		}
		else if ( m_min == null ) {
			return m_max.compareTo(v) >= 0;
		}
		else if ( m_max == null ) {
			return m_min.compareTo(v) <= 0;
		}
		else {
			return m_min.compareTo(v) <= 0 && m_max.compareTo(v) >= 0;
		}
	}
	
	public Range<T> span(Range<T> other) {
		return new Range<>(min(m_min, other.m_min), max(m_max, other.m_max));
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		@SuppressWarnings("unchecked")
		Range<T> other = (Range<T>)obj;
		return m_min.equals(other.m_min) && m_max.equals(other.m_max);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_min, m_max);
	}

	@Override
	public int compareTo(Range<T> other) {
		int cmp = m_min.compareTo(other.m_min);
		if ( cmp != 0 ) {
			return cmp;
		}
		
		return m_max.compareTo(other.m_max);
	}

	public int compareTo(T other) {
		if ( m_min.compareTo(other) > 0 ) {
			return 1;
		}
		else if ( m_max.compareTo(other) < 0 ) {
			return -1;
		}
		else {
			return 0;
		}
	}
	
	@Override
	public String toString() {
		String minStr = (m_min != null) ? ""+m_min : "inf";
		String maxStr = (m_max != null) ? ""+m_max : "inf";
		
		return String.format("[%s..%s]", minStr, maxStr);
	}
	
	private static <T extends Comparable<T>> T min(T v1, T v2) {
		if ( v1 == null || v2 == null ) {
			return null;
		}
		else {
			return (v1.compareTo(v2) < 0) ? v1 : v2;
		}
	}
	
	private static <T extends Comparable<T>> T max(T v1, T v2) {
		if ( v1 == null || v2 == null ) {
			return null;
		}
		else {
			return (v1.compareTo(v2) > 0) ? v1 : v2;
		}
	}
}