package jarvey.streams.model;

import java.util.Objects;

import jarvey.streams.zone.ZoneLineRelation;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class ZoneRelation {
	private final ZoneLineRelation m_relation;
	private final String m_zoneId;
	
	private ZoneRelation(ZoneLineRelation rel, String zoneId) {
		m_zoneId = zoneId;
		m_relation = rel;
	}
	
	private static final ZoneRelation _UNASSIGNED = new ZoneRelation(ZoneLineRelation.Unassigned, null);
	public static ZoneRelation UNASSIGNED() {
		return _UNASSIGNED;
	}
	
	public static ZoneRelation ENTERED(String zone) {
		return new ZoneRelation(ZoneLineRelation.Entered, zone);
	}
	
	public static ZoneRelation LEFT(String zone) {
		return new ZoneRelation(ZoneLineRelation.Left, zone);
	}
	
	public static ZoneRelation INSIDE(String zone) {
		return new ZoneRelation(ZoneLineRelation.Inside, zone);
	}
	
	public static ZoneRelation THROUGH(String zone) {
		return new ZoneRelation(ZoneLineRelation.Through, zone);
	}

	private static final ZoneRelation _DELETED = new ZoneRelation(ZoneLineRelation.Deleted, null);
	public static ZoneRelation DELETED() {
		return _DELETED;
	}
	
	public boolean isEntered() {
		return m_relation.equals(ZoneLineRelation.Entered);
	}
	
	public boolean isLeft() {
		return m_relation.equals(ZoneLineRelation.Left);
	}
	
	public boolean isInside() {
		return m_relation.equals(ZoneLineRelation.Inside);
	}
	
	public boolean isThrough() {
		return m_relation.equals(ZoneLineRelation.Through);
	}
	
	public boolean isDeleted() {
		return m_relation.equals(ZoneLineRelation.Deleted);
	}
	
	public ZoneLineRelation getRelation() {
		return m_relation;
	}
	
	public String getZoneId() {
		return m_zoneId;
	}

	public static ZoneRelation parse(String relStr) {
		try {
			relStr = relStr.trim();
			switch ( relStr.charAt(0) ) {
				case 'U':
					return UNASSIGNED();
				case 'D':
					return DELETED();
				default:
					break;
			}
			
			char rel = relStr.charAt(0);
			String zoneId = relStr.substring(2, relStr.length()-1);
			switch ( rel ) {
				case 'I':
					return INSIDE(zoneId);
				case 'E':
					return ENTERED(zoneId);
				case 'L':
					return LEFT(zoneId);
				case 'T':
					return THROUGH(zoneId);
				default:
					throw new IllegalArgumentException(String.format("invalid ZoneRelation: %s", relStr));
			}
		}
		catch ( Exception e ) {
			e.printStackTrace();
			return null;
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		ZoneRelation other = (ZoneRelation)obj;
		return Objects.equals(m_zoneId, other.m_zoneId) && m_relation == other.m_relation;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_relation, m_zoneId);
	}
	
	@Override
	public String toString() {
		if ( m_zoneId != null ) {
			return String.format("%s(%s)", m_relation.getSymbol(), m_zoneId);
		}
		else {
			return String.format("%s", m_relation.getSymbol());
		}
	}
}