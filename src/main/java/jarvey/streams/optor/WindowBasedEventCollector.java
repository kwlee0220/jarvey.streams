package jarvey.streams.optor;

import java.util.List;

import com.google.common.collect.Lists;

import jarvey.streams.Aggregator;
import jarvey.streams.model.Timestamped;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class WindowBasedEventCollector<T extends Timestamped>
				extends WindowBasedAggregator<T, Aggregator<T,List<T>>, List<T>> {
	public WindowBasedEventCollector(HoppingWindowManager windowMgr) {
		super(windowMgr, EventCollector::new);
	}
	
	private static class EventCollector<T extends Timestamped> implements Aggregator<T,List<T>> {
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
