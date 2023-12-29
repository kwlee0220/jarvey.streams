package jarvey.streams.rx;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RxKafkas {
	static final Logger s_logger = LoggerFactory.getLogger(RxKafkas.class);
	
	private RxKafkas() {
		throw new AssertionError("Should not be called: class=" + RxKafkas.class);
	}
	
	public static class PublishResult<K,V> {
		private final RecordMetadata m_metadata;
		private final ProducerRecord<K,V> m_record;
		private final Exception m_error;
		
		PublishResult(RecordMetadata metadata, ProducerRecord<K,V> record, Exception error) {
			m_metadata = metadata;
			m_record = record;
			m_error = error;
		}
		
		public RecordMetadata getMetadata() {
			return m_metadata;
		}
		
		public ProducerRecord<K,V> getProducerRecord() {
			return m_record;
		}
		
		public Exception getException() {
			return m_error;
		}
	}
	
	public static <K,V> Observable<PublishResult<K,V>>
	produce(@Nonnull KafkaProducer<K, V> producer, @Nonnull Flowable<ProducerRecord<K,V>> records) {
		PublishSubject<PublishResult<K,V>> results = PublishSubject.create();
		records.subscribe(rec -> producer.send(rec, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				results.onNext(new PublishResult<>(metadata, rec, exception));
			}
		}));
		
		return results;
	}
}
