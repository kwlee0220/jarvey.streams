/**
 * 
 */
package jarvey.streams.serialization.json;

import java.io.IOException;

import org.locationtech.jts.geom.Point;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import utils.geo.util.GeometryUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PointAdapter extends TypeAdapter<Point> {
	@Override
	public Point read(JsonReader in) throws IOException {
		double[] xy = GsonUtils.readDoubleArray(in);
		return GeometryUtils.toPoint(xy[0], xy[1]);
	}

	@Override
	public void write(JsonWriter out, Point pt) throws IOException {
		GsonUtils.writeDoubleArray(out, pt.getX(), pt.getY());
	}
}
