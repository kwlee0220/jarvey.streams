package jarvey.streams.model;

import org.apache.kafka.common.serialization.Serde;

import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.NodeTrackletIndex;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveySerdes {
	private static final Serde<TrackletId> s_TrackletIdSerde = GsonUtils.getSerde(TrackletId.class);
	private static final Serde<NodeTrack> s_nodeTrackSerde = GsonUtils.getSerde(NodeTrack.class);
	private static final Serde<NodeTrackletIndex> s_nodeTrackletIndexSerde
														= GsonUtils.getSerde(NodeTrackletIndex.class);
	private static final Serde<BinaryAssociation> s_binaryAssociationSerde
															= GsonUtils.getSerde(BinaryAssociation.class);
	private static final Serde<AssociationClosure> s_associationClosureSerde
														= GsonUtils.getSerde(AssociationClosure.class);
	private static final Serde<GlobalTrack> s_globalTrackSerde = GsonUtils.getSerde(GlobalTrack.class);
	
	public static Serde<TrackletId> TrackletId() {
		return s_TrackletIdSerde;
	}
	
	public static Serde<NodeTrack> NodeTrack() {
		return s_nodeTrackSerde;
	}
	
	public static Serde<NodeTrackletIndex> NodeTrackletIndex() {
		return s_nodeTrackletIndexSerde;
	}
	
	public static Serde<BinaryAssociation> BinaryAssociation() {
		return s_binaryAssociationSerde;
	}
	
	public static Serde<AssociationClosure> AssociationClosure() {
		return s_associationClosureSerde;
	}
	
	public static Serde<GlobalTrack> GlobalTrack() {
		return s_globalTrackSerde;
	}
	
	public static Serde<TrackFeature> TrackFeature() {
		return TrackFeatureSerde.s_serde;
	}
}