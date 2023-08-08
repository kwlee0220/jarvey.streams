package jarvey.streams;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.Utilities;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HoppingWindowManager {
	private static final Logger s_logger = LoggerFactory.getLogger(HoppingWindowManager.class);
	
	private String m_name = "HoppingWindow";
	private final long m_windowSize;
	private long m_advanceTime;
	private long m_graceTime;

	private long m_currentTs = -1;
	private final List<Window> m_windows = Lists.newArrayList();
	
	public static HoppingWindowManager ofWindowSize(Duration windowSize) {
		return new HoppingWindowManager(windowSize);
	}
	
	private HoppingWindowManager(Duration windowSize) {
		m_windowSize = windowSize.toMillis();
		m_advanceTime = m_windowSize;
		m_graceTime = 0;
	}
	
	public HoppingWindowManager ofName(String name) {
		m_name = name;
		return this;
	}
	
	public HoppingWindowManager advanceTime(Duration dur) {
		Utilities.checkArgument(dur.toMillis() > 0,
								() -> String.format("invalid advance size: %s", dur));
		m_advanceTime = dur.toMillis();
		return this;
	}
	
	public HoppingWindowManager graceTime(Duration dur) {
		Utilities.checkArgument(dur.toMillis() > 0,
								() -> String.format("invalid advance size: %s", dur));
		
		m_graceTime = dur.toMillis();
		return this;
	}
	
	public long getCurrentTime() {
		return m_currentTs;
	}
	
	private Window addNewWindow(long beginTs) {
		Window window = new Window(beginTs, m_windowSize);
		m_windows.add(window);
		
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("add a new window: {}", window);
		}
		
		return window;
	}
	
	public Tuple<List<Window>, List<Window>> collect(long ts) {
		List<Window> closeds = Collections.emptyList();
		if ( ts > m_currentTs ) {
			m_currentTs = ts;
			
			// Grace time까지 고려할 때, close 시킬 window를 찾는다.
			closeds = FStream.from(m_windows)
							.takeWhile(w -> w.endTime()+m_graceTime <= ts)
							.peek(Window::close)
							.toList();
			if ( closeds.size() > 0 ) {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("close windows: {}", closeds);
				}
				m_windows.removeAll(closeds);
			}
			
			Window last = Funcs.asNonNull(Funcs.getLast(m_windows), () -> addNewWindow(ts));
			while ( true ) {
				long nextWindowBeginTs = last.beginTime() + m_advanceTime;
				if ( ts < nextWindowBeginTs ) {
					break;
				}
				
				last = addNewWindow(nextWindowBeginTs);
			}
		}
		else if ( ts < m_currentTs ) {
			Window window = Funcs.getFirst(m_windows);
			if ( window != null && window.beginTime() > ts ) {
				// too-late data
				if ( s_logger.isWarnEnabled() ) {
					s_logger.warn("{}: lost too-late data: ts={}, earlest window={}", this, ts, window);
				}
			}
		}
		
		// 시간 범위가 맞는 window들에 aggregation을 시도한다.
		List<Window> updateds = FStream.from(m_windows)
										.dropWhile(w -> w.isAfter(ts))
										.takeWhile(w -> w.contains(ts))
										.toList();
		
		return Tuple.of(closeds, updateds);
	}
	
	@Override
	public String toString() {
		String advanceStr = (m_advanceTime != m_windowSize)
							? String.format(",advance=%s", Duration.ofMillis(m_advanceTime)) : "";
		String graceStr = (m_graceTime > 0)
							? String.format(",grace=%s", Duration.ofMillis(m_graceTime)) : "";
		String windowsStr = FStream.from(m_windows)
									.map(Window::toString)
									.join(",", "{", "}");
		
		return String.format("%s[size=%s%s%s]#%d - %s", m_name,
								Duration.ofMillis(m_windowSize), advanceStr, graceStr,
								m_currentTs, windowsStr);
	}
}
