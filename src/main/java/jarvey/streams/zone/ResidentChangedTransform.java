/**
 * 
 */
package jarvey.streams.zone;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.streams.UpdateTimeAssociatedKeyValue;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ResidentChangedTransform implements ValueTransformer<ZoneLineRelationEvent, Iterable<ResidentChanged>> {
	private static final Logger s_logger = LoggerFactory.getLogger(AdjustZoneLineCrossTransform.class);
	
	private static final Duration DEFAULT_TTL_MINUTES = Duration.ofMinutes(5);
	private static final int DEFAULT_CHECKUP_MINUTES = 3;
	
	private final String m_storeName;
	private UpdateTimeAssociatedKeyValue<GlobalZoneId, Residents> m_residents;
	
	ResidentChangedTransform(String storeName) {
		m_storeName = storeName;
	}

	@Override
	public void init(ProcessorContext context) {
		KeyValueStore<GlobalZoneId, Residents> store = context.getStateStore(m_storeName);
		m_residents = UpdateTimeAssociatedKeyValue.of(store);
		m_residents.setLogger(s_logger);
		
		context.schedule(Duration.ofMinutes(DEFAULT_CHECKUP_MINUTES),
						PunctuationType.WALL_CLOCK_TIME,
						ts -> m_residents.deleteOldEntries(DEFAULT_TTL_MINUTES));
	}

	@Override
	public Iterable<ResidentChanged> transform(ZoneLineRelationEvent ev) {
		String nodeId = ev.getNodeId();
		GlobalZoneId gzone = new GlobalZoneId(nodeId, ev.getZone());
		
		Residents residents;
		switch ( ev.getRelation() ) {
			case Entered:
				residents = m_residents.get(gzone);
				if ( residents == null ) {
					residents = new Residents();
				}
				residents = residents.update(ev);
				if ( residents != null ) {
					m_residents.put(gzone, residents);
					return Arrays.asList(ResidentChanged.from(gzone, residents));
				}
				else {
					return Collections.emptyList();
				}
			case Left:
				residents = m_residents.get(gzone);
				if ( residents == null ) {
					residents = new Residents(Collections.emptySet(), ev.getFrameIndex(), ev.getTimestamp());
					m_residents.put(gzone, residents);
					return Collections.emptyList();
				}
				else {
					residents = residents.update(ev);
					if ( residents != null ) {
						m_residents.put(gzone, residents);
						return Arrays.asList(ResidentChanged.from(gzone, residents));
					}
					else {
						return Collections.emptyList();
					}
				}
			case Deleted:
				residents = m_residents.get(gzone);
				if ( residents != null ) {
					m_residents.delete(gzone);
				}
				return Collections.emptyList();
			case Through:
				return Collections.emptyList();
			default:
				throw new AssertionError("ZoneLineCross event have a invalid state: ev=" + ev);
		}
	}

	@Override
	public void close() { }
}
