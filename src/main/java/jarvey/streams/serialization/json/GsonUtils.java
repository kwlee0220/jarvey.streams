/**
 * 
 */
package jarvey.streams.serialization.json;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.kafka.common.serialization.Serde;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.Point;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GsonUtils {
	private GsonUtils() {
		throw new AssertionError("should not be called: class=" + GsonUtils.class);
	}
	
	private static final Gson s_gson;
	static {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(Point.class, new PointAdapter());
		builder.registerTypeAdapter(Envelope.class, new EnvelopeAdater());
		builder.registerTypeAdapter(LineSegment.class, new LineSegmentAdapter());
		s_gson = builder.create();
	}
	
	public static Gson getGson() {
		return s_gson;
	}
	
	public static <T> Serde<T> getSerde(Class<T> cls) {
		return new JsonKafkaSerde<>(cls, s_gson);
	}
	
	public static <T> T parseJson(String gsonStr, Class<T> cls) {
		return s_gson.fromJson(gsonStr, cls);
	}
	
	public static String toJson(Object obj) {
		return s_gson.toJson(obj);
	}
	
	public static <T> T parseJson(String gsonStr, Type type) {
		return s_gson.fromJson(gsonStr, type);
	}
	
	public static <T> List<T> parseList(String gsonStr, Class<T> cls) {
		Type listType = new TypeToken<List<T>>() {}.getType();
		return new Gson().fromJson(gsonStr, listType);
	}
	
	public static <K,V> List<GsonKeyValue<K,V>> parseKVList(String gsonStr, Class<K> keyCls, Class<V> valCls) {
		Type listType = new TypeToken<List<GsonKeyValue<K,V>>>() {}.getType();
		return new Gson().fromJson(gsonStr, listType);
	}

	public static double[] readNullableDoubleArray(JsonReader in) throws IOException {
		if ( in.peek() == JsonToken.NULL ) {
			in.nextNull();
			return null;
		}
		else {
			return readDoubleArray(in);
		}
	}

	public static double[] readDoubleArray(JsonReader in) throws IOException {
		in.beginArray();
		
		List<Double> vList = Lists.newArrayList();
		while ( in.peek().equals(JsonToken.NUMBER) ) {
			vList.add(in.nextDouble());
		}
		
		in.endArray();
		
		return vList.stream().mapToDouble(d->d).toArray();
	}
	
	public static void writeNullableDoubleArray(JsonWriter out, double... values) throws IOException {
		if ( values != null ) {
			writeDoubleArray(out, values);
		}
		else {
			if ( !out.getSerializeNulls() ) {
				out.setSerializeNulls(true);
				out.nullValue();
				out.setSerializeNulls(false);
			}
			else {
				out.nullValue();
			}
		}
	}
	
	public static void writeDoubleArray(JsonWriter out, double... values) throws IOException {
		out.beginArray();
		
		for ( double v: values ) {
			out.value(v);
		}
		
		out.endArray();
	}
}
