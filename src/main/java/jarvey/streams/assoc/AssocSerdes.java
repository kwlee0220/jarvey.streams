package jarvey.streams.assoc;

import org.apache.kafka.common.serialization.Serde;

import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssocSerdes {
	private static final Serde<BinaryAssociation> s_binaryAssociationSerde
															= GsonUtils.getSerde(BinaryAssociation.class);
	private static final Serde<Association> s_associationClosureSerde
														= GsonUtils.getSerde(Association.class);
	private static final Serde<GlobalTrack> s_globalTrackSerde = GsonUtils.getSerde(GlobalTrack.class);
	
	public static Serde<BinaryAssociation> BinaryAssociation() {
		return s_binaryAssociationSerde;
	}
	
	public static Serde<Association> AssociationClosure() {
		return s_associationClosureSerde;
	}
	
	public static Serde<GlobalTrack> GlobalTrack() {
		return s_globalTrackSerde;
	}
}