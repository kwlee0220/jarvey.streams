package jarvey.streams.zone;

import static utils.Utilities.checkState;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.streams.UpdateTimeAssociatedKeyValueStore;
import utils.func.FOption;
import utils.func.Funcs;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TrackZoneLocationsUpdater implements ValueTransformer<ZoneEvent, Iterable<ZoneEvent>> {
	private static final Logger s_logger = LoggerFactory.getLogger(TrackZoneLocationsUpdater.class);
	
	private static final Duration DEFAULT_TTL_MINUTES = Duration.ofMinutes(5);
	private static final int DEFAULT_CHECKUP_MINUTES = 3;
	
	private final String m_storeName;
	private UpdateTimeAssociatedKeyValueStore<String, TrackZoneLocations> m_zoneLocations;
//	private KeyValueStore<String, TrackZoneLocations> m_zoneLocations;
	
	TrackZoneLocationsUpdater(String storeName) {
		m_storeName = storeName;
	}

	@Override
	public void init(ProcessorContext context) {
		KeyValueStore<String, TrackZoneLocations> store = context.getStateStore(m_storeName);
		m_zoneLocations = UpdateTimeAssociatedKeyValueStore.of(store);
		context.schedule(Duration.ofMinutes(DEFAULT_CHECKUP_MINUTES),
						PunctuationType.WALL_CLOCK_TIME,
						ts -> m_zoneLocations.deleteOldEntries(DEFAULT_TTL_MINUTES));
	}

	@Override
	public Iterable<ZoneEvent> transform(ZoneEvent zev) {
		String trackId = zev.getTrackId();
		String zone = zev.getZoneId();
		long ts = zev.getTimestamp();
		
		TrackZoneLocations zlocs = FOption.ofNullable(m_zoneLocations.get(trackId))
										.getOrElse(() -> TrackZoneLocations.empty(trackId, ts));
		
		if ( zev.isDeleted() ) {
			m_zoneLocations.delete(trackId);
			
			// 이전 존재하던 모든 zone에서 left하는 이벤틀 생성한다.
			return FStream.from(zlocs.getZoneIds())
							.map(zid -> LEFT(zev, zid))
							.peek(left -> s_logger.info("insert {} before deletion", left))
							.concatWith(zev)
							.toList();
		}
		
		Set<String> newZoneIds;
		switch ( zev.getRelation() ) {
			case Unassigned:
				// 이전 위치가 어떤 zone에도 포함되지 않았어야 한다.
				checkState(zlocs.size() == 0);
				newZoneIds = Collections.emptySet();
				break;
			case Left:
				// 추적 대상 물체가 left한 zone에 이미 존재했어야 한다.
				checkState(zlocs.contains(zev.getZoneId()),
							() -> String.format("track(%s) has not been in the zone(%s)", trackId, zone));
				newZoneIds = Funcs.removeCopy(zlocs.getZoneIds(), zone);
				break;
			case Entered:
				// 추적 대상 물체가 enter한 zone에 존재하지 않았어야 한다.
				checkState(!zlocs.contains(zone));
				newZoneIds = Funcs.addCopy(zlocs.getZoneIds(), zone);
				break;
			case Inside:
				// 추적 대상 물체가 left한 zone에 이미 존재했어야 한다.
				checkState(zlocs.contains(zev.getZoneId()));
				newZoneIds = zlocs.getZoneIds();
				break;
			case Through:
				// 추적 대상 물체가 enter한 zone에 존재하지 않았어야 한다.
				checkState(!zlocs.contains(zone));
				newZoneIds = zlocs.getZoneIds();
				break;
			default:
				throw new AssertionError(String.format("unexpected %s event state: state=%s",
													zev.getClass().getSimpleName(), zev.getRelation()));
		}
		
		m_zoneLocations.put(trackId, new TrackZoneLocations(trackId, newZoneIds, ts));
		return Arrays.asList(zev);
	}

	@Override
	public void close() { }
	
	private static ZoneEvent LEFT(ZoneEvent zev, String zid) {
		return new ZoneEvent(zev.getTrackId(), ZoneLineRelation.Left, zid, zev.getTimestamp());
	}
}
