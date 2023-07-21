package jarvey.streams.zone;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import utils.func.FOption;
import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.streams.UpdateTimeAssociatedKeyValue;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AdjustZoneLineCrossTransform implements ValueTransformer<ZoneLineRelationEvent, Iterable<MergedLocationEvent>> {
	private static final Logger s_logger = LoggerFactory.getLogger(AdjustZoneLineCrossTransform.class);
	
	private static final Duration DEFAULT_TTL_MINUTES = Duration.ofMinutes(5);
	private static final int DEFAULT_CHECKUP_MINUTES = 3;
	
	private final String m_storeName;
	private UpdateTimeAssociatedKeyValue<TrackletId, ZoneLocations> m_zoneLocations;
	
	AdjustZoneLineCrossTransform(String storeName) {
		m_storeName = storeName;
	}

	@Override
	public void init(ProcessorContext context) {
		KeyValueStore<TrackletId, ZoneLocations> store = context.getStateStore(m_storeName);
		m_zoneLocations = UpdateTimeAssociatedKeyValue.of(store);
		context.schedule(Duration.ofMinutes(DEFAULT_CHECKUP_MINUTES),
						PunctuationType.WALL_CLOCK_TIME,
						ts -> m_zoneLocations.deleteOldEntries(DEFAULT_TTL_MINUTES));
	}

	@Override
	public Iterable<MergedLocationEvent> transform(ZoneLineRelationEvent relEvent) {
		TrackletId guid = relEvent.getGUID();
		Set<String> zoneIds = FOption.ofNullable(m_zoneLocations.get(guid))
									.map(ZoneLocations::getZoneIds)
									.getOrElse(Sets::newHashSet);
		switch ( relEvent.getRelation() ) {
			case Unassigned:
				if ( zoneIds.size() > 0 ) {
					s_logger.warn("invalid zone locations: event={}, locations={}", relEvent, zoneIds);
					
					// 이전에 소속되었던 모든 zone에 대해 Left event를 발생시킨다.
					List<ZoneLineRelationEvent> events = FStream.from(zoneIds)
														.map(zone -> create(relEvent, ZoneLineRelation.Left, zone))
														.concatWith(relEvent)
														.toList();
					ZoneLocations zoneLocs = new ZoneLocations(Collections.emptySet(),
																relEvent.getFrameIndex(), relEvent.getTimestamp());
					m_zoneLocations.put(guid, zoneLocs);

					LocationChanged changed = LocationChanged.from(guid, zoneLocs);
					return MergedLocationEvent.from(events, changed);
				}
				else {
					return MergedLocationEvent.from(relEvent);
				}
			case Left:
				if ( zoneIds.contains(relEvent.getZone()) ) {
					Set<String> newZoneIds = Funcs.remove(zoneIds, relEvent.getZone());
					ZoneLocations updateLocs = new ZoneLocations(newZoneIds, relEvent.getFrameIndex(),
																	relEvent.getTimestamp());
					m_zoneLocations.put(guid, updateLocs);
					
					LocationChanged changed = LocationChanged.from(guid, updateLocs);
					return MergedLocationEvent.from(relEvent, changed);
				}
				else {
					// 첫번째 line이 zone에서 밖으로 나는 경우 가짜로 Entered 이벤트를 추가한다.
					s_logger.info("LEFT without the previous ENTERED, insert fake ENTERED: node={}, luid={}, zone={}",
								relEvent.getGUID().getNodeId(), relEvent.getTrackId(), relEvent.getZone());
					ZoneLineRelationEvent dummyEntered = create(relEvent, ZoneLineRelation.Entered,
																	relEvent.getZone());
					return MergedLocationEvent.from(Arrays.asList(dummyEntered, relEvent));
				}
			case Entered:
				if ( !zoneIds.contains(relEvent.getZone()) ) {
					Set<String> newZoneIds = Funcs.add(zoneIds, relEvent.getZone());
					ZoneLocations updateLocs = new ZoneLocations(newZoneIds, relEvent.getFrameIndex(),
																relEvent.getTimestamp());
					m_zoneLocations.put(guid, updateLocs);
					
					LocationChanged changed = LocationChanged.from(guid, updateLocs);
					return MergedLocationEvent.from(relEvent, changed);
				}
				else {
					s_logger.warn("invalid zone locations: event={}, locations={}", relEvent, zoneIds);
					return Collections.emptyList();
				}
			case Inside:
				if ( !zoneIds.contains(relEvent.getZone()) ) {
					s_logger.info("INSIDE without the previous ENTERED, insert fake ENTERED: node={}, luid={}, zone={}",
									relEvent.getGUID().getNodeId(), relEvent.getTrackId(), relEvent.getZone());
					Set<String> newZoneIds = Funcs.add(zoneIds, relEvent.getZone());
					ZoneLocations updateLocs = new ZoneLocations(newZoneIds, relEvent.getFrameIndex(),
																relEvent.getTimestamp());
					m_zoneLocations.put(guid, updateLocs);
					
					ZoneLineRelationEvent dummyEntered = create(relEvent, ZoneLineRelation.Entered);
					LocationChanged changed = LocationChanged.from(guid, updateLocs);
					return MergedLocationEvent.from(Arrays.asList(dummyEntered, relEvent), changed);
				}
				else {
					return MergedLocationEvent.from(relEvent);
				}
			case Through:
				if ( zoneIds.contains(relEvent.getZone()) ) {
					s_logger.warn("invalid zone locations: event={}, locations={}", relEvent, zoneIds);

					Set<String> newZoneIds = Funcs.remove(zoneIds, relEvent.getZone());
					ZoneLocations updateLocs = new ZoneLocations(newZoneIds, relEvent.getFrameIndex(),
																	relEvent.getTimestamp());
					m_zoneLocations.put(guid, updateLocs);

					ZoneLineRelationEvent left = create(relEvent, ZoneLineRelation.Left);
					LocationChanged changed = LocationChanged.from(guid, updateLocs);
					return MergedLocationEvent.from(left, changed);
				}
				else {
					return MergedLocationEvent.from(relEvent);
				}
			case Deleted:
				m_zoneLocations.delete(guid);
				
				// LEFT가 없이 추적 물체가 delete된 경우에는 가짜로 LEFT event를 추가한다.
				if ( zoneIds.size() > 0 ) {
					List<ZoneLineRelationEvent> events
						= FStream.from(zoneIds)
								.peek((zone) -> s_logger.info("insert dummy LEFT for the deleted object: guid={}, zone={}",
																guid, zone))
								.map(zone -> create(relEvent, ZoneLineRelation.Left, zone))
								.concatWith(relEvent)
								.toList();
					ZoneLocations updateLocs = new ZoneLocations(Collections.emptySet(), relEvent.getFrameIndex(),
																	relEvent.getTimestamp());

					LocationChanged changed = LocationChanged.from(guid, updateLocs);
					return MergedLocationEvent.from(events, changed);
				}
				else {
					return MergedLocationEvent.from(relEvent);
				}
			default:
				throw new AssertionError(String.format("unexpected %s event state: state=%s",
													relEvent.getClass().getSimpleName(), relEvent.getRelation()));
		}
	}

	@Override
	public void close() { }
	
	private static ZoneLineRelationEvent create(ZoneLineRelationEvent org, ZoneLineRelation rel) {
		return new ZoneLineRelationEvent(org.getNodeId(), org.getTrackId(), rel, org.getZone(), org.getLine(),
								org.getFrameIndex(), org.getTimestamp());
	}
	private static ZoneLineRelationEvent create(ZoneLineRelationEvent org, ZoneLineRelation rel, String zone) {
		return new ZoneLineRelationEvent(org.getNodeId(), org.getTrackId(), rel, zone, org.getLine(),
								org.getFrameIndex(), org.getTimestamp());
	}
}
