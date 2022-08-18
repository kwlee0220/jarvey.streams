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
		double[] xy = GsonUtils.readNullableDoubleArray(in);
		return (xy != null) ? GeometryUtils.toPoint(xy[0], xy[1]) : null;
	}

	@Override
	public void write(JsonWriter out, Point pt) throws IOException {
		double[] xy = (pt != null) ? new double[] {pt.getX(), pt.getY()} : null;
		GsonUtils.writeNullableDoubleArray(out, xy);
	}
}
