package jarvey.streams;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.Maps;

import jarvey.streams.model.Timestamped;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class WindowBasedAggregation<T extends Timestamped, A extends Aggregator<T,R>, R> {
	private final HoppingWindowManager m_windowMgr;
	private final Map<Window,A> m_aggrs = Maps.newHashMap();
	private final Function<Window,A> m_aggrFactory;
	
	public WindowBasedAggregation(HoppingWindowManager windowMgr, Function<Window,A> aggrFact) {
		m_windowMgr = windowMgr;
		m_aggrFactory = aggrFact;
	}
	
	public List<Windowed<R>> collect(T event) {
		Tuple<List<Window>, List<Window>> tup = m_windowMgr.collect(event.getTimestamp());
		
		List<Window> assigneds = tup._2;
		for ( Window window: assigneds ) {
			A aggr = m_aggrs.computeIfAbsent(window, m_aggrFactory::apply);
			aggr.aggregate(event);
		}
		
		List<Window> expireds = tup._1;
		List<Windowed<R>> results
			= FStream.from(expireds)
					.filter(m_aggrs::containsKey)
					.map(w -> Windowed.of(w, m_aggrs.get(w).close()))
					.toList();
		for ( Windowed<R> r: results ) {
			m_aggrs.remove(r.window());
		}
		
		return results;
	}
	
	private FOption<Windowed<R>> toWindowed(Window window) {
		return FOption.ofNullable(m_aggrs.get(window))
						.map(r -> Windowed.of(window, r.close()));
	}
}
