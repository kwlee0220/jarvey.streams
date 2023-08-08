package jarvey.streams.serialization.json;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import jarvey.streams.model.NodeTrack;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NodeTrackStateAdapter extends TypeAdapter<NodeTrack.State> {
	@Override
	public NodeTrack.State read(JsonReader in) throws IOException {
		return NodeTrack.State.valueOf(in.nextString());
	}

	@Override
	public void write(JsonWriter out, NodeTrack.State state) throws IOException {
		out.value(state.name());
	}
}
