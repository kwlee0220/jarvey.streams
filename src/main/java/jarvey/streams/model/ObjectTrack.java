package jarvey.streams.model;

import org.locationtech.jts.geom.Point;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ObjectTrack extends Timestamped {
	public String getId();
	public Point getLocation();
	public boolean isDeleted();
}
