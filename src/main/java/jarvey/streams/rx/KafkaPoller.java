package jarvey.streams.rx;

import static utils.Utilities.checkNotNullArgument;

import java.time.Duration;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import utils.func.FOption;
import utils.stream.FStream;
import utils.stream.FStreams.AbstractFStream;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KafkaPoller {
	private final Duration m_pollTimeout;
	private final @Nullable Duration m_initialPollTimeout;	// null 값 여부에 따라 initial poll 여부를 알 수 있음.
	private final boolean m_stopOnTimeout;
	
	private KafkaPoller(@Nonnull Duration timeout, Duration initialIimeout, boolean stopOnTimeout) {
		m_pollTimeout = timeout;
		m_initialPollTimeout = initialIimeout;
		m_stopOnTimeout = stopOnTimeout;
	}
	
	public static KafkaPoller infinite(Duration timeout) {
		return new KafkaPoller(timeout, null, false);
	}
	
	public static KafkaPoller stopOnTimeout(Duration timeout) {
		return new KafkaPoller(timeout, null, true);
	}
	
	public KafkaPoller initialTimeout(Duration initialIimeout) {
		return new KafkaPoller(m_pollTimeout, initialIimeout, m_stopOnTimeout);
	}
	
	<K,V> FStream<ConsumerRecord<K,V>> start(@Nonnull KafkaConsumer<K, V> consumer,
											 @Nonnull Consumer<KafkaConsumer<K, V>> closer) {
		return new Polling<>(consumer, closer);
	}
	
	<K,V> FStream<ConsumerRecord<K,V>> start(@Nonnull KafkaConsumer<K, V> consumer) {
		return new Polling<>(consumer, c->{});
	}
	
	private class Polling<K,V> extends AbstractFStream<ConsumerRecord<K,V>> { 
		private final KafkaConsumer<K, V> m_consumer;
		private final Consumer<KafkaConsumer<K, V>> m_closer;
		private FStream<ConsumerRecord<K,V>> m_inner = FStream.empty();	// null if closed
		private boolean m_initial = true;
		
		Polling(@Nonnull KafkaConsumer<K, V> consumer, @Nonnull Consumer<KafkaConsumer<K, V>> closer) {
			checkNotNullArgument(consumer);
			checkNotNullArgument(closer);
			
			m_consumer = consumer;
			m_closer = closer;
		}

		@Override
		protected void closeInGuard() throws Exception {
			if ( m_inner != null ) {
				m_inner = null;
				
				m_closer.accept(m_consumer);
			}
		}
		
		@Override
		public FOption<ConsumerRecord<K,V>> nextInGuard() {
			if ( m_inner == null ) {
				throw new IllegalStateException("closed already");
			}
			
			return m_inner.next()
							.orElse(() -> {
								m_inner = pollNextStream();
								return m_inner.next();
							});
		}
		
		private FStream<ConsumerRecord<K,V>> pollNextStream() {
			while ( true ) {
				Duration pollTimeout = (m_initial && m_initialPollTimeout != null)
										? m_initialPollTimeout
										: m_pollTimeout;
				m_initial = false;
				
				ConsumerRecords<K, V> records = m_consumer.poll(pollTimeout);
				if ( records.count() > 0 ) {
					return FStream.from(records);
				}
				if ( m_stopOnTimeout ) {
					return FStream.empty();
				}
			}
		}
	}
}
