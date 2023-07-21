package jarvey.streams.serialization.json;

import java.io.IOException;

import org.locationtech.jts.geom.Envelope;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import utils.geo.util.GeometryUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeAdater extends TypeAdapter<Envelope> {
	@Override
	public Envelope read(JsonReader in) throws IOException {
		double[] tlbr = GsonUtils.readDoubleArray(in);
		return GeometryUtils.toEnvelope(tlbr[0], tlbr[1], tlbr[2], tlbr[3]);
	}

	@Override
	public void write(JsonWriter out, Envelope envl) throws IOException {
		if ( envl != null ) {
			GsonUtils.writeDoubleArray(out, envl.getMinX(), envl.getMinY(), envl.getMaxX(), envl.getMaxY());
		}
		else {
			out.nullValue();
		}
	}
}
