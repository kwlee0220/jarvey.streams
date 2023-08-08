package jarvey.streams.zone;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import com.google.common.base.Objects;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Zone {
	private final String m_id;
	private final Polygon m_region;
	
	public Zone(String id, Polygon region) {
		m_id = id;
		m_region = region;
	}
	
	public String getId() {
		return m_id;
	}
	
	public boolean intersects(Geometry geom) {
		return m_region.intersects(geom);
	}
	
	public Geometry intersection(Geometry geom) {
		return m_region.intersection(geom);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null  || obj.getClass() != Zone.class ) {
			return false;
		}
		
		Zone other = (Zone)obj;
		return Objects.equal(m_id, other.m_id);
	}
	
	@Override
	public int hashCode() {
		return m_id.hashCode();
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s", m_id, m_region);
	}
}