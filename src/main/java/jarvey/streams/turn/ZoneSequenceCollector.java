/**
 * 
 */
package jarvey.streams.turn;

import java.time.Duration;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.streams.UpdateTimeAssociatedKeyValue;
import jarvey.streams.model.GUID;
import jarvey.streams.zone.ZoneLineRelationEvent;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ZoneSequenceCollector implements ValueTransformer<ZoneLineRelationEvent, ZoneSequence> {
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
	public ZoneSequence transform(ZoneLineRelationEvent ev) {
		GUID guid = ev.getGUID();
		ZoneSequence seq = m_store.get(guid);
		
		switch ( ev.getRelation() ) {
			case Entered:
				if ( seq == null ) {
					seq = ZoneSequence.from(ev);
				}
				else {
					ZoneTravel travel = seq.getLastZoneTravel();
					if ( travel.isOpen() ) {
						throw new IllegalStateException("unexpected input event: " + ev + ", last-travel=" + travel);
					}
					seq.append(ZoneTravel.open(ev));
				}
				m_store.put(guid, seq);
				break;
			case Left:
				if ( seq == null ) {
					s_logger.warn("see LEFT, but empty sequence, event={}", ev);
					ZoneTravel first = ZoneTravel.open(ev).close(ev.getFrameIndex(), ev.getTimestamp());
					seq = ZoneSequence.from(ev.getNodeId(), ev.getLuid(), first);
					m_store.put(guid, seq);
				}
				else {
					ZoneTravel travel = seq.getLastZoneTravel();
					if ( travel.getZoneId().equals(ev.getZone()) || travel.isOpen() ) {
						travel.close(ev.getFrameIndex(), ev.getTimestamp());
						m_store.put(guid, seq);
					}
					else {
						throw new IllegalStateException("unexpected input event: " + ev + ", last-travel=" + travel);
					}
				}
				break;
			case Through:
				if ( seq == null ) {
					seq = ZoneSequence.from(ev);
					seq.getLastZoneTravel().close(ev.getFrameIndex(), ev.getTimestamp());
				}
				else {
					ZoneTravel last = seq.getLastZoneTravel();
					if ( last.isClosed() ) {
						ZoneTravel travel = ZoneTravel.open(ev);
						travel.close(ev.getFrameIndex(), ev.getTimestamp());
						seq.append(travel);
					}
					else {
						throw new IllegalStateException("unexpected input event: " + ev + ", last-travel=" + last);
					}
				}
				m_store.put(guid, seq);
				break;
			case Deleted:
				m_store.delete(guid);
				break;
			default:
				throw new IllegalArgumentException("unexpected input event: " + ev);
		}
		
		return seq;
	}

	@Override
	public void close() { }
}
