package jarvey.streams.node;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class NodeTrackIndexBuildProcessor implements Processor<String, NodeTrack, String, NodeTrack> {
	private ProcessorContext<String,NodeTrack> m_context;
	private final NodeTrackletIndexManager m_idxMgr;
	
	public NodeTrackIndexBuildProcessor(NodeTrackletIndexManager idxMgr) {
		m_idxMgr = idxMgr;
	}

	@Override
	public void init(ProcessorContext<String,NodeTrack> context) {
		m_context = context;
	}

	@Override
	public void process(Record<String, NodeTrack> record) {
		NodeTrack track = record.value();
		RecordMetadata meta = m_context.recordMetadata().get();
		
		m_idxMgr.update(track, meta.partition(), meta.offset());
	}
}
