package jarvey.streams.turn;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.streams.kstream.ValueMapper;

import com.google.common.collect.Maps;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TurnDetector implements ValueMapper<ZoneSequence, Iterable<ObjectTurn>> {
	private static final Map<String,String> TURN_SIGNATURES = Maps.newHashMap();
	static {
		TURN_SIGNATURES.put("[zone01-zone02)", "straight");
		TURN_SIGNATURES.put("[zone01-zone03)", "right");
		TURN_SIGNATURES.put("[zone02-zone01)", "straight");
		TURN_SIGNATURES.put("[zone02-zone03)", "left");
		TURN_SIGNATURES.put("[zone03-zone01)", "left");
		TURN_SIGNATURES.put("[zone03-zone02)", "right");
	}
	
	public TurnDetector() { }

	@Override
	public Iterable<ObjectTurn> apply(ZoneSequence seq) {
		String sig = toSignature(seq);
		String turn = TURN_SIGNATURES.get(sig);
		if ( turn != null ) {
			return Arrays.asList(new ObjectTurn(seq.getNodeId(), seq.getLuid(), turn));
		}
		else {
			return Collections.emptyList();
		}
	}
	
	private String toSignature(ZoneSequence seq) {
		if ( seq.getVisitCount() > 0 ) {
			String visitStr = FStream.from(seq.getZoneIdSequence()).join('-');
			String endDelim = seq.getLastZoneTravel().isClosed() ? "]" : ")";
			return String.format("[%s%s", visitStr, endDelim);
		}
		else {
			return "";
		}
	}
}
