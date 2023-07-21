package jarvey.streams;

import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractTransformer<K,V,R> implements Transformer<K, V, R> {
	@Override
	public void init(ProcessorContext context) {
	}

	@Override
	public void close() {
	}
}
