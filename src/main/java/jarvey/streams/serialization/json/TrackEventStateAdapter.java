package jarvey.streams.serialization.json;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import jarvey.streams.model.TrackEvent;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TrackEventStateAdapter extends TypeAdapter<TrackEvent.State> {
	@Override
	public TrackEvent.State read(JsonReader in) throws IOException {
		return TrackEvent.State.valueOf(in.nextString());
	}

	@Override
	public void write(JsonWriter out, TrackEvent.State state) throws IOException {
		out.value(state.name());
	}
}
