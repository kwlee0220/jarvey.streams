package jarvey.streams.serialization.json;

import java.io.IOException;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineSegment;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import utils.geo.util.GeoUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class LineSegmentAdapter extends TypeAdapter<LineSegment> {
	@Override
	public LineSegment read(JsonReader in) throws IOException {
		double[] coords = GsonUtils.readNullableDoubleArray(in);
		if ( coords != null ) {
			Coordinate p0 = new Coordinate(coords[0], coords[1]);
			Coordinate p1 = new Coordinate(coords[2], coords[3]);
			return GeoUtils.toLineSegment(p1, p0);
		}
		else {
			return null;
		}
	}

	@Override
	public void write(JsonWriter out, LineSegment seg) throws IOException {
		if ( seg != null ) {
			GsonUtils.writeDoubleArray(out, seg.p0.x, seg.p0.y, seg.p1.x, seg.p1.y);
		}
		else {
			GsonUtils.writeNullableDoubleArray(out, null);
		}
	}
}
