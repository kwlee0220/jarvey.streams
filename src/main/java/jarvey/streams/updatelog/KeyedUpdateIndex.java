package jarvey.streams.updatelog;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;

import jarvey.streams.model.Range;
import jarvey.streams.model.Timestamped;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class KeyedUpdateIndex implements Timestamped {
	@SerializedName("key") private String m_key;
	@SerializedName("partition") private int m_partition;
	@SerializedName("offset_range") private Range<Long> m_offsetRange;
	@SerializedName("ts_range") private Range<Long> m_tsRange;
	@SerializedName("length") private int m_count;
	
	public KeyedUpdateIndex(String key, int partitioNo,  Range<Long> offsetRange,
							Range<Long> tsRange, int count) {
		m_key = key;
		m_partition = partitioNo;
		m_offsetRange = offsetRange;
		m_tsRange = tsRange;
		m_count = count;
	}
	
	public String getKey() {
		return m_key;
	}
	
	public int getPartitionNumber() {
		return m_partition;
	}
	
	public Range<Long> getTopicOffsetRange() {
		return m_offsetRange;
	}
	
	public Range<Long> getTimestampRange() {
		return m_tsRange;
	}

	@Override
	public long getTimestamp() {
		return m_tsRange.max();
	}
	
	public int getUpdateCount() {
		return m_count;
	}
	
	public void setLastUpdate(long offset) {
		Long min = m_offsetRange.min();
		m_offsetRange.set(Range.between(min, offset));
	}
	
	public boolean isClosed() {
		return !m_offsetRange.isInfiniteMax();
	}
	
	public void update(long ts) {
		m_tsRange.expand(ts);
		m_count += 1;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		KeyedUpdateIndex other = (KeyedUpdateIndex)obj;
		return Objects.equals(m_key, other.m_key);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_key);
	}
	
	@Override
	public String toString() {
		return String.format("%s: {part=%d, offsets=%s, ts=%s, length=%d}",
								getKey(), m_partition, m_offsetRange, m_tsRange, m_count);
	}
}