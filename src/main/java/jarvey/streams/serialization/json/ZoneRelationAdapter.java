package jarvey.streams.serialization.json;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import jarvey.streams.model.ZoneRelation;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ZoneRelationAdapter extends TypeAdapter<ZoneRelation> {
	@Override
	public ZoneRelation read(JsonReader in) throws IOException {
		String zoneRelStr = in.nextString();
		return ZoneRelation.parse(zoneRelStr);
	}

	@Override
	public void write(JsonWriter out, ZoneRelation rel) throws IOException {
		out.value(rel.toString());
	}
}
