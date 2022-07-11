/**
 * 
 */
package jarvey.streams.process.old;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import jarvey.streams.model.ObjectTrack;
import jarvey.streams.model.old.ZonedTrackDAO;
import utils.geo.util.GeoUtils;
import utils.stream.KVFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AttachZoneTransform implements ValueMapper<ObjectTrack, ZonedTrackDAO> {
	private final Map<String,Polygon> m_zones;
	
	AttachZoneTransform(Map<String,Polygon> zones) {
		m_zones = zones;
	}

	@Override
	public ZonedTrackDAO apply(ObjectTrack ev) {
		if ( ev.isDeleted() ) {
			return ZonedTrackDAO.DELETED(ev);
		}
		
		Point centroid = GeoUtils.getCentroid(ev.getBox());
		Set<String> zoneIds = KVFStream.from(m_zones)
									.filterValue(zone -> zone.intersects(centroid))
									.toKeyStream()
									.toSet();
		if ( zoneIds.size() > 0 ) {
			return ZonedTrackDAO.MATCHED(ev, zoneIds);
		}
		else {
			return ZonedTrackDAO.UNMATCHED(ev);
		}
	}
}
