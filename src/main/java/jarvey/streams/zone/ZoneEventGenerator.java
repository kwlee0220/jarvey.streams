package jarvey.streams.zone;

import static utils.Utilities.checkArgument;
import static utils.Utilities.checkState;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

import com.google.common.collect.Lists;

import utils.func.FLists;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ZoneEventGenerator implements ValueMapper<LineTrack, Iterable<ZoneEvent>> {
	private final Map<String,Zone> m_zones;
	
	ZoneEventGenerator(List<Zone> zones) {
		checkArgument(zones.size() > 0, "# of zones is zero");
		
		m_zones = FStream.from(zones).toMap(z -> z.getId());
	}

	@Override
	public Iterable<ZoneEvent> apply(LineTrack lineTrack) {
		if ( lineTrack.isDeleted() ) {
			return Arrays.asList(DELETED(lineTrack));
		}
		else if ( lineTrack.isPointTrack() ) {
			// Point track인 경우 (즉, track의 첫번째 track event인 경우)
			Point pt = lineTrack.getPoint();
			List<ZoneEvent> result =  FStream
					.from(m_zones.values())
					.filter(zone -> zone.intersects(pt))
					.map(zone -> new ZoneEvent(lineTrack.getId(), ZoneLineRelation.Entered,
												zone.getId(), lineTrack.getTimestamp()))
					.toList();
			checkState(result.size() <= 1);
			if ( result.size() == 1 ) {
				return result;
			}
			else {
				return Arrays.asList(UNASSIGNED(lineTrack));
			}
		}
		else {
			return handleLineTrack(lineTrack);
		}
	}
	
	private List<ZoneEvent> handleLineTrack(LineTrack lineTrack) {
		LineString line = GeoUtils.toLineString(lineTrack.getLine());
		List<ZoneEvent> zoneEvents =  FStream
											.from(m_zones.values())
											.filter(zone -> zone.intersects(line))
											.map(zone -> toZoneEvent(zone, lineTrack))
											.toList();
		if ( zoneEvents.size() == 0 ) {
			return Collections.emptyList();
		}
		else if ( zoneEvents.size() == 1 ) {
			return zoneEvents;
		}
		else {
			// 한 line에 여러 zone을 걸쳐 지나기 때문에, 각각 발생하는 zone event의
			// 발생 순서를 정한다.
			
			List<ZoneEvent> outEvents = Lists.newArrayList();
			
			List<List<ZoneEvent>> branches
					= FLists.branch(zoneEvents,
									zev -> zev.getRelation() == ZoneLineRelation.Left,
									zev -> zev.getRelation() == ZoneLineRelation.Through,
									zev -> zev.getRelation() == ZoneLineRelation.Entered);

			// 일단 left event가 존재하는가 확인하여 이를 첫번째로 삽입함.
			List<ZoneEvent> leftEvents = branches.get(0);
			checkState(leftEvents.size() <= 1);
			outEvents.addAll(leftEvents);
			
			// through event를 찾아내어 다음으로 삽입한다.
			// 만일 2개 이상의 through event가 존재하는 경우에는 line의 시작점을
			// 기준으로 through된 zone과의 거리를 구한 후, 짧은 순서로 정렬시켜 event를 삽입함
			List<ZoneEvent> thruEvents = branches.get(1);
			if ( thruEvents.size() == 1 ) {
				outEvents.add(thruEvents.get(0));
			}
			else if ( thruEvents.size() > 1 ) {
				FStream.from(thruEvents)
						.sort((ev1, ev2) -> Double.compare(distanceToZone(line, ev1),
															distanceToZone(line, ev2)))
						.forEach(outEvents::add);
				
			}
			
			// 마지막으로 enter event가 존재하는가 확인하여 이들을 발송함.
			List<ZoneEvent> enterEvents = branches.get(2);
			checkState(enterEvents.size() <= 1);
			outEvents.addAll(enterEvents);
			
			return outEvents;
		}
	}
	
	private double distanceToZone(LineString line, ZoneEvent zev) {
		Zone zone = m_zones.get(zev.getZoneId());
		Geometry overlap = zone.intersection(line);
		return line.getStartPoint().distance(overlap);
	}
	
	private static ZoneEvent toZoneEvent(Zone zone, LineTrack track) {
		return new ZoneEvent(track.getId(), toZoneLineCross(zone, track),
								zone.getId(), track.getTimestamp());
	}
	
	private static ZoneLineRelation toZoneLineCross(Zone zone, LineTrack track) {
		LineSegment line = track.getLine();
		boolean startCond = zone.intersects(GeoUtils.toPoint(line.p0));
		boolean endCond = zone.intersects(GeoUtils.toPoint(line.p1));
		if ( startCond && endCond ) {
			return ZoneLineRelation.Inside;
		}
		else if ( !startCond && endCond ) {
			return ZoneLineRelation.Entered;
		}
		else if ( startCond && !endCond ) {
			return ZoneLineRelation.Left;
		}
		else {
			return ZoneLineRelation.Through;
		}
	}

	private static ZoneEvent DELETED(LineTrack track) {
		return new ZoneEvent(track.getId(), ZoneLineRelation.Deleted, null, track.getTimestamp());
	}

	private static ZoneEvent UNASSIGNED(LineTrack track) {
		return new ZoneEvent(track.getId(), ZoneLineRelation.Unassigned, null, track.getTimestamp());
	}
}
