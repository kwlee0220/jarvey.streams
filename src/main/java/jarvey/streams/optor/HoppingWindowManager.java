package jarvey.streams.optor;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.Preconditions;
import utils.Tuple;
import utils.func.Funcs;
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
	
	public Duration getWindowSize() {
		return Duration.ofMillis(m_windowSize);
	}
	
	public HoppingWindowManager advanceTime(Duration dur) {
		Preconditions.checkArgument(dur.toMillis() > 0, "invalid advance size: %s", dur);
		m_advanceTime = dur.toMillis();
		return this;
	}
	
	public Duration advanceTime() {
		return Duration.ofMillis(m_advanceTime);
	}
	
	public HoppingWindowManager graceTime(Duration dur) {
		Preconditions.checkArgument(dur.toMillis() >= 0, "invalid advance size: %s", dur);
		
		m_graceTime = dur.toMillis();
		return this;
	}
	
	public Duration graceTime() {
		return Duration.ofMillis(m_graceTime);
	}
	
	public long getLastTimestamp() {
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
	
	public List<Window> close() {
		List<Window> windows = Lists.newArrayList(m_windows);
		m_windows.clear();
		
		return windows;
	}
	
	public Tuple<List<Window>, List<Window>> collect(long ts) {
		List<Window> expireds = Collections.emptyList();
		if ( ts > m_currentTs ) {
			m_currentTs = ts;
			
			// Grace time까지 고려할 때, close 시킬 window를 찾는다.
			expireds = FStream.from(m_windows)
							.takeWhile(w -> w.endTime()+m_graceTime <= ts)
							.peek(Window::close)
							.toList();
			if ( expireds.size() > 0 ) {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("expired windows: {}", expireds);
				}
				m_windows.removeAll(expireds);
			}
			
			// 새로 입력된 timestamp를 기준으로 추가로 생성되어야할 window가 필요한가
			// 검사하여 필요한 window들을 생성하여 추가한다.
			// 입력 timestamp가 많이 커진 경우에는 여러 개의 window가 생성될 수도 있다.
			Window last = Funcs.getLast(m_windows);
			if ( last == null ) {
				// 최초 입력 timestamp인 경우에는, 입력 timestamp가 포함되는 window를 생성하여 추가한다.
				last = addNewWindow(ts);
			}
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
										.filter(w -> w.compareTo(ts) == 0)
										.toList();
		
		return Tuple.of(expireds, updateds);
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
