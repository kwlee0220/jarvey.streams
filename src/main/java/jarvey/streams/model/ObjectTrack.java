package jarvey.streams.model;

import org.locationtech.jts.geom.Point;

import jarvey.streams.updatelog.KeyedUpdate;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ObjectTrack extends KeyedUpdate {
	public Point getLocation();
	public boolean isDeleted();
	
	public default boolean isLastUpdate() {
		return isDeleted();
	}
}
