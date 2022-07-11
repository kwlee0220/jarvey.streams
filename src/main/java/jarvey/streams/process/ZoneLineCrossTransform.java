/**
 * 
 */
package jarvey.streams.process;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import com.google.common.collect.Maps;

import jarvey.streams.model.LineTrack;
import jarvey.streams.model.ZoneLineCross;
import utils.func.KeyValue;
import utils.geo.util.GeoUtils;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;
import utils.stream.KVFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ZoneLineCrossTransform implements ValueMapper<LineTrack, Iterable<ZoneLineCross>> {
	private final JdbcProcessor m_jdbc;
	private final Map<String, Map<String,Polygon>> m_zoneGroups;
	
	ZoneLineCrossTransform(JdbcProcessor jdbc) {
		m_jdbc = jdbc;
		
		try {
			m_zoneGroups = loadZoneGroups(m_jdbc);
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Iterable<ZoneLineCross> apply(LineTrack ev) {
		if ( ev.isDeleted() ) {
			return Arrays.asList(DELETED(ev));
		}
		
		Map<String,Polygon> zoneGroup = m_zoneGroups.get(ev.getNodeId());
		LineString line = GeoUtils.toLineString(ev.getLine());
		List<ZoneLineCross> assignments = KVFStream.from(zoneGroup)
													.filterValue(zone -> zone.intersects(line))
													.mapValue(zone -> getCrossType(zone, line))
													.map(kv -> toZoneLineCross(ev, kv))
													.toList();
		if ( assignments.size() > 0 ) {
			return assignments;
		}
		else {
			return Arrays.asList(UNASSIGNED(ev));
		}
	}
	
	private ZoneLineCross toZoneLineCross(LineTrack track, KeyValue<String,String> kv) {
		return new ZoneLineCross(track.getNodeId(), track.getLuid(), kv.value(), kv.key(),
									track.getLine(), track.getFrameIndex(), track.getTimestamp());
	}
	
	private String getCrossType(Polygon zone, LineString line) {
		boolean startCond = zone.intersects(line.getStartPoint());
		boolean endCond = zone.intersects(line.getEndPoint());
		if ( startCond && endCond ) {
			return ZoneLineCross.STATE_INSIDE;
		}
		else if ( !startCond && endCond ) {
			return ZoneLineCross.STATE_ENTERED;
		}
		else if ( startCond && !endCond ) {
			return ZoneLineCross.STATE_LEFT;
		}
		else {
			return ZoneLineCross.STATE_THROUGH;
		}
	}

	private static ZoneLineCross DELETED(LineTrack track) {
		return new ZoneLineCross(track.getNodeId(), track.getLuid(), ZoneLineCross.STATE_DELETED,
									null, null, track.getFrameIndex(), track.getTimestamp());
	}

	private static ZoneLineCross UNASSIGNED(LineTrack track) {
		return new ZoneLineCross(track.getNodeId(), track.getLuid(), ZoneLineCross.STATE_UNASSIGNED,
									null, track.getLine(), track.getFrameIndex(), track.getTimestamp());
	}
	
	public static Map<String, Map<String,Polygon>> loadZoneGroups(JdbcProcessor jdbc)
		throws SQLException, ClassNotFoundException {
		Map<String, Map<String,Polygon>> zoneGroups = Maps.newHashMap();
		try ( ResultSet rset = jdbc.executeQuery("select node, zone, area from node_zones") ) {
			while ( rset.next() ) {
				String nodeId = rset.getString("node");
				String zoneId = rset.getString("zone");
				String coordsStr = rset.getString("area");
				Coordinate[] shell = FStream.of(coordsStr.split(","))
											.map(Double::parseDouble)
											.buffer(2, 2)
											.map(vals -> new Coordinate(vals.get(0), vals.get(1)))
											.toArray(Coordinate.class);
				Polygon zone = GeoUtils.toPolygon(shell);
				Map<String,Polygon> group = zoneGroups.computeIfAbsent(nodeId, k -> Maps.newHashMap());
				group.put(zoneId, zone);
			}
		}
		
		return zoneGroups;
	}
}
