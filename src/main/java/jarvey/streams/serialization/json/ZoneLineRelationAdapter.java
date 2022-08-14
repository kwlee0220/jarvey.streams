package jarvey.streams.serialization.json;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import jarvey.streams.zone.ZoneLineRelation;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ZoneLineRelationAdapter extends TypeAdapter<ZoneLineRelation> {
	@Override
	public ZoneLineRelation read(JsonReader in) throws IOException {
		return ZoneLineRelation.valueOf(in.nextString());
	}

	@Override
	public void write(JsonWriter out, ZoneLineRelation rel) throws IOException {
		out.value(rel.name());
	}
}
