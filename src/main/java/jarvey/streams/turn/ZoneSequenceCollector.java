package jarvey.streams.turn;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.streams.UpdateTimeAssociatedKeyValue;
import jarvey.streams.model.GUID;
import jarvey.streams.zone.GlobalZoneId;
import jarvey.streams.zone.ZoneLineRelationEvent;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ZoneSequenceCollector implements ValueTransformer<ZoneLineRelationEvent, Iterable<ZoneSequence>> {
	private static final Logger s_logger = LoggerFactory.getLogger(ZoneSequenceCollector.class);

	private static final Duration DEFAULT_TTL_MINUTES = Duration.ofMinutes(5);
	private static final int DEFAULT_CHECKUP_MINUTES = 3;

	private final String m_storeName;
	private UpdateTimeAssociatedKeyValue<GUID, ZoneSequence> m_store;
	
	public ZoneSequenceCollector(String storeName) {
		m_storeName = storeName;
	}

	@Override
	public void init(ProcessorContext context) {
		KeyValueStore<GUID, ZoneSequence> store = context.getStateStore(m_storeName);
		m_store = UpdateTimeAssociatedKeyValue.of(store);
		context.schedule(Duration.ofMinutes(DEFAULT_CHECKUP_MINUTES),
						PunctuationType.WALL_CLOCK_TIME,
						ts -> m_store.deleteOldEntries(DEFAULT_TTL_MINUTES));
	}

	@Override
	public Iterable<ZoneSequence> transform(ZoneLineRelationEvent ev) {
		GUID guid = ev.getGUID();
		
		List<ZoneSequence> ret = Lists.newArrayList();
		
		// 동일 물체에 대한 이전 zone sequence 정보를 얻는다.
		// 만일 이전 정보가 없는 경우에는 null이 반환됨.
		ZoneSequence seq = m_store.get(guid);
//		System.err.println(ev);
		
		ZoneTravel last = null;
		switch ( ev.getRelation() ) {
			case Entered:
				if ( seq == null || seq.getVisitCount() == 0 ) {
					// 현 물체에 대한 첫 zone 방문이므로, zone sequence 객체를 새로 생성
					seq = ZoneSequence.empty(ev.getNodeId(), ev.getLuid());
				}
				else {
					last = seq.getLastZoneTravel();
					if ( last.isOpen() ) {
						// travel의 open이라는 뜻은 현재 해당 물체가 어떤 zone 안에 있다는 의미이기 때문에,
						// 뭔가 inconsistent한 상태임.
						// 만일 현 zone과 이벤트의 zone이 서로 다른 경우에는 현 zone에서 나온 이벤트가
						// 발생한 효과를 주고, 두 zone이 같은 경우에는 이 이벤트를 무시함.
						GlobalZoneId current = new GlobalZoneId(ev.getNodeId(), last.getZoneId());
						s_logger.warn(String.format("see ENTERED(%s), but already in the zone(%s)",
													ev.getGlobalZoneId(), current));
						if ( !last.getZoneId().equals(ev.getGlobalZoneId().getZoneId() )) {
							last.close(ev.getFrameIndex(), ev.getTimestamp());
							ret.add(seq.duplicate());
						}
						else {
							return Collections.emptyList();
						}
					}
				}
				seq.append(ZoneTravel.open(ev));
				m_store.put(guid, seq);
				
				ret.add(seq);
				return ret;
			case Left:
				if ( seq == null || seq.getVisitCount() == 0 ) {
					// 추적 대상 물체가 처음 시작하는 단계이므로 zone sequence 객체를 생성함.
					seq = ZoneSequence.empty(ev.getNodeId(), ev.getLuid());
				}
				
				last = seq.getLastZoneTravel();
				if ( last == null || last.isClosed() ) {
					// 물체가 현재 아무런 zone에도 위치하지 않은 경우.
					// left 이벤트에 해당하는 zone에 들어왔다는 이벤트를 사전에 추가함.
					s_logger.warn(String.format("see LEFT(%s), but is not in any zone now",
													ev.getGlobalZoneId()));
					last = ZoneTravel.open(ev);
					seq.append(last);
					ret.add(seq.duplicate());
				}

				if ( !last.getZoneId().equals(ev.getZone()) ) {
					// 물체의 현재 위치한 zone과 left event의 zone과 서로 다른 경우에는
					// 물체가 현재 위치한 zone에서 나오고 event의 zone에 진입한 상태를 강제로 만든다.
					GlobalZoneId current = new GlobalZoneId(ev.getNodeId(), last.getZoneId());
					s_logger.warn(String.format("see LEFT(%s), but in the zone(%s) now",
												ev.getGlobalZoneId(), current));
					
					// 현재 위치한 zone에서 left한 이벤트 추가
					last.close(ev.getFrameIndex(), ev.getTimestamp());
					ret.add(seq.duplicate());
					
					//  Left 이벤트에 해당하는 zone에 들어왔다는 이벤트 추가.
					last = ZoneTravel.open(ev);
					seq.append(last);
					ret.add(seq.duplicate());
				}

				// 모든 사전 조건을 맞추어 놓은 상태.
				// 현재 위치한 zone에서 close 시킴.
				last.close(ev.getFrameIndex(), ev.getTimestamp());
				
				// state store 상태 변경
				m_store.put(guid, seq);
				
				ret.add(seq);
				return ret;
			case Through:
				if ( seq == null || seq.getVisitCount() == 0 ) {
					// 현 물체에 대한 첫 zone 방문이므로, zone sequence 객체를 새로 생성
					seq = ZoneSequence.empty(ev.getNodeId(), ev.getLuid());
				}
				else {
					last = seq.getLastZoneTravel();
					if ( last.isOpen() ) {
						// travel의 open이라는 뜻은 현재 해당 물체가 어떤 zone 안에 있다는 의미이기 때문에,
						// inconsistent한 상태임.
						// 현 zone에서 left한 이벤트를 추가함.
						GlobalZoneId current = new GlobalZoneId(ev.getNodeId(), last.getZoneId());
						s_logger.warn(String.format("see THROUGH(%s), but already in the zone(%s)",
													ev.getGlobalZoneId(), current));
						last.close(ev.getFrameIndex(), ev.getTimestamp());
						ret.add(seq.duplicate());

						// 만일 추적 대상 물체가 위치했던 zone이 현 through 이벤트의 zone과 동일하다면
						// 더 이상의 작업없이 바로 반환.
						if ( last.getZoneId().equals(ev.getGlobalZoneId().getZoneId() )) {
							m_store.put(guid, seq);
							return ret;
						}
					}
				}

				last = ZoneTravel.open(ev);
				seq.append(last);
				ret.add(seq.duplicate());
				
				last.close(ev.getFrameIndex(), ev.getTimestamp());
				ret.add(seq);
				
				// state store 상태 변경
				m_store.put(guid, seq);
				return ret;
			case Deleted:
				m_store.delete(guid);
				
				if ( seq != null && seq.getVisitCount() > 0 ) {
					last = seq.getLastZoneTravel();
					if ( last.isOpen() ) {
						s_logger.warn(String.format("object(%s) was disappeared without left zone(%s)",
														guid, last.getZoneId()));
						last.close(ev.getFrameIndex(), ev.getTimestamp());
						ret.add(seq);
					}
				}
				return ret;
			default:
				throw new IllegalArgumentException("unexpected input event: " + ev);
		}
	}

	@Override
	public void close() { }
}
