package jarvey.streams;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Maps;

import utils.func.Tuple;
import utils.stream.FStream;

import jarvey.streams.model.Timestamped;


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
	
	public WindowBasedAggregation(HoppingWindowManager windowMgr, Supplier<A> aggSupplier) {
		m_windowMgr = windowMgr;
		m_aggrFactory = w -> aggSupplier.get();
	}
	
	public List<Windowed<R>> close() {
		List<Window> expireds = m_windowMgr.close();
		List<Windowed<R>> results = getExpiredAggregation(expireds);
		m_aggrs.clear();
		
		return results;
	}
	
	public List<Windowed<R>> collect(T event) {
		Tuple<List<Window>, List<Window>> tup = m_windowMgr.collect(event.getTimestamp());
		List<Window> expireds = tup._1;
		List<Window> assigneds = tup._2;
		
		for ( Window window: assigneds ) {
			A aggr = m_aggrs.computeIfAbsent(window, m_aggrFactory::apply);
			aggr.aggregate(event);
		}
		
		List<Windowed<R>> results = getExpiredAggregation(expireds);
		
		// expire된 window에 해당하는 aggregator 객체를 삭제한다.
		for ( Windowed<R> r: results ) {
			m_aggrs.remove(r.window());
		}
		
		return results;
	}
	
	public List<Windowed<R>> progress(long timestamp) {
		Tuple<List<Window>, List<Window>> tup = m_windowMgr.collect(timestamp);
		List<Window> expireds = tup._1;
		
		List<Windowed<R>> results = getExpiredAggregation(expireds);
		
		// expire된 window에 해당하는 aggregator 객체를 삭제한다.
		for ( Windowed<R> r: results ) {
			m_aggrs.remove(r.window());
		}
		
		return results;
	}
	
	private List<Windowed<R>> getExpiredAggregation(List<Window> expiredWindows) {
		List<Windowed<R>> results = FStream.from(expiredWindows)
											.filter(m_aggrs::containsKey)
											.map(w -> Windowed.of(w, m_aggrs.get(w).close()))
											.toList();
		return results;
	}
}
