package jarvey.streams.model;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;
import com.google.common.primitives.Floats;

import jarvey.streams.node.TrackFeature;

import dna.node.proto.TrackFeatureProto;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TrackFeatureSerde implements Serde<TrackFeature> {
	private static final Serializer<TrackFeatureProto> s_protoSerializer = new KafkaProtobufSerializer<>();
	private static final Deserializer<TrackFeatureProto> s_protoDeserializer
											= new KafkaProtobufDeserializer<>(TrackFeatureProto.parser());
	
	public static final TrackFeatureSerde s_serde = new TrackFeatureSerde();
	public static final Serializer<TrackFeature> s_serializer = new TrackFeatureSerializer();
	public static final Deserializer<TrackFeature> s_deerializer = new TrackFeatureDeserializer();

	@Override
	public Serializer<TrackFeature> serializer() {
		return s_serializer;
	}

	@Override
	public Deserializer<TrackFeature> deserializer() {
		return s_deerializer;
	}
	
	private static class TrackFeatureSerializer implements Serializer<TrackFeature> {
		@Override
		public byte[] serialize(String topic, TrackFeature feature) {
			TrackFeatureProto.Builder builder = TrackFeatureProto.newBuilder()
																.setNodeId(feature.getNodeId())
																.setTrackId(feature.getTrackId())
																.setFrameIndex(feature.getFrameIndex())
																.setTs(feature.getTimestamp());
			if ( feature != null ) {
				builder.addAllFeature(Floats.asList(feature.getFeature()));
			}
			TrackFeatureProto proto = builder.build();
			return s_protoSerializer.serialize(topic, proto);
		}
	}

	private static class TrackFeatureDeserializer implements Deserializer<TrackFeature> {
		@Override
		public TrackFeature deserialize(String topic, byte[] data) {
			TrackFeatureProto proto = s_protoDeserializer.deserialize(topic, data);
			
			float[] feature = proto.getFeatureCount() > 0 ? Floats.toArray(proto.getFeatureList()) : null;
			return new TrackFeature(proto.getNodeId(), proto.getTrackId(), feature,
										proto.getFrameIndex(), proto.getTs());
		}
	}
}
