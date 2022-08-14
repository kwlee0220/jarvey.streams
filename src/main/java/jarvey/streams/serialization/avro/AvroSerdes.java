package jarvey.streams.serialization.avro;

import java.util.Collections;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroSerdes {
	public static <T extends SpecificRecord> Serde<T> getSerde(String url, boolean isKey) {
		Map<String,String> serdeConfig = Collections.singletonMap("schema.registry.url", url);
		SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
		serde.configure(serdeConfig, isKey);
		return serde;
	}
}
