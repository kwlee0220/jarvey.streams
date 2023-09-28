package jarvey.streams.zone;

import static utils.Utilities.checkState;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.func.FOption;
import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.streams.UpdateTimeAssociatedKeyValueStore;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ResidentsGenerator implements ValueTransformer<ZoneEvent, Iterable<Residents>> {
	private static final Logger s_logger = LoggerFactory.getLogger(ResidentsGenerator.class);
	
	private static final Duration DEFAULT_TTL_MINUTES = Duration.ofMinutes(5);
	private static final int DEFAULT_CHECKUP_MINUTES = 3;
	
	private final String m_storeName;
	private UpdateTimeAssociatedKeyValueStore<String, Residents> m_residents;
	
	ResidentsGenerator(String storeName) {
		m_storeName = storeName;
	}

	@Override
	public void init(ProcessorContext context) {
		KeyValueStore<String, Residents> store = context.getStateStore(m_storeName);
		m_residents = UpdateTimeAssociatedKeyValueStore.of(store);
		m_residents.setLogger(s_logger);
		
		context.schedule(Duration.ofMinutes(DEFAULT_CHECKUP_MINUTES),
						PunctuationType.WALL_CLOCK_TIME,
						ts -> m_residents.deleteOldEntries(DEFAULT_TTL_MINUTES));
	}

	@Override
	public Iterable<Residents> transform(ZoneEvent zev) {
		String trackId = zev.getTrackId();
		String zoneId = zev.getZoneId();
		long ts = zev.getTimestamp();
		
		Set<String> residents;
		Residents outEv;
		switch ( zev.getRelation() ) {
			case Entered:
				residents = FOption.ofNullable(m_residents.get(zoneId))
									.map(Residents::getTrackIds)
									.getOrElse(Collections::emptySet);
				if ( residents == null ) {
					residents = Collections.singleton(trackId); 
				}
				else {
					checkState(residents.contains(trackId));
					residents = Funcs.addCopy(residents, trackId);
				}
				
				outEv = new Residents(zoneId, residents, ts);
				m_residents.put(zoneId, outEv);
				
				return Arrays.asList(outEv);
			case Left:
			case Through:
				residents = FOption.ofNullable(m_residents.get(zoneId))
									.map(Residents::getTrackIds)
									.getOrElse(Collections::emptySet);
				if ( residents.contains(trackId) ) {
					residents = Funcs.removeCopy(residents, trackId);
					outEv = new Residents(zoneId, residents, ts);
					m_residents.put(zoneId, outEv);
					
					return Arrays.asList(outEv);
				}
				else {
					return Collections.emptyList();
				}
			case Deleted:
				try ( KeyValueIterator<String, Residents> iter = m_residents.all() ) {
					List<KeyValue<String,Residents>> updateds
							= FStream.from(iter)
									.map(kv -> kv.value)
									.filter(r -> r.contains(trackId))
									.map(r -> r.remove(trackId, ts))
									.map(r -> KeyValue.pair(r.getZoneId(), r))
									.toList();
					m_residents.putAll(updateds);
					
					return FStream.from(updateds).map(kv -> kv.value).toList();
				}
			case Unassigned:
				return Collections.emptyList();
			default:
				throw new AssertionError("ZoneLineCross event have a invalid state: ev=" + zev);
		}
	}

	@Override
	public void close() { }
}
