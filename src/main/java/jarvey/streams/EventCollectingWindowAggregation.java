package jarvey.streams;

import java.util.List;

import com.google.common.collect.Lists;

import jarvey.streams.model.Timestamped;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EventCollectingWindowAggregation<T extends Timestamped>
				extends WindowBasedAggregation<T, Aggregator<T,List<T>>, List<T>> {
	public EventCollectingWindowAggregation(HoppingWindowManager windowMgr) {
		super(windowMgr, EventCollectingAggregator::new);
	}
	
	private static class EventCollectingAggregator<T extends Timestamped> implements Aggregator<T,List<T>> {
		private final List<T> m_coll = Lists.newArrayList();
		
		@Override
		public void aggregate(T data) {
			m_coll.add(data);
		}

		@Override
		public List<T> close() {
			return m_coll;
		}
		
		@Override
		public String toString() {
			if ( m_coll.size() > 0 ) {
				long firstTs = m_coll.get(0).getTimestamp();
				long lastTs = m_coll.get(m_coll.size()-1).getTimestamp();
				double dur = (lastTs - firstTs) / 1000;
				
				return String.format("%d[%d-%d:%.1f]", m_coll.size(), firstTs, lastTs, dur);
			}
			else {
				return "[]";
			}
		}
	}
}
