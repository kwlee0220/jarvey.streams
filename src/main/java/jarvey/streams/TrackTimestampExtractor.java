package jarvey.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import jarvey.streams.model.Timestamped;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TrackTimestampExtractor implements TimestampExtractor {
	@Override
	public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
		if ( record.value() != null && record.value() instanceof Timestamped ) {
			Timestamped stamped = (Timestamped)record.value();
			return stamped.getTimestamp();
		}
		
		return partitionTime;
	}
}
