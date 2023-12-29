package jarvey.streams.assoc;

import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import utils.LoggerSettable;
import utils.func.FOption;
import utils.stream.FStream;

import jarvey.streams.model.TrackletId;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class ReactiveAssociationCache implements KafkaTopicPartitionProcessor<String,byte[]>, LoggerSettable {
	private final Logger s_logger = LoggerFactory.getLogger(ReactiveAssociationCache.class);
	
	private final AssociationCache m_cache;
	private final Set<TrackletId> m_wanteds = Sets.newHashSet();
	private Logger m_logger;
	
	public ReactiveAssociationCache(AssociationCache cache) {
		m_cache = cache;
		
		setLogger(s_logger);
	}

	@Override
	public void close() throws Exception { }
	
	public FOption<Association> getAssociation(TrackletId trkId) {
		if ( m_wanteds.contains(trkId) ) {
			return FOption.empty();
		}
		
		return FOption.ofNullable(m_cache.getOrLoad(trkId))
						.ifAbsent(() ->{
							if ( getLogger().isInfoEnabled() ) {
								getLogger().info("{} blocks cache", trkId);
							}
							m_wanteds.add(trkId);
						});
	}
	
	public void putAssociation(TrackletId trkId, Association assoc) {
		m_wanteds.remove(trkId);
		m_cache.put(trkId, assoc);
	}

	@Override
	public ProcessResult process(TopicPartition tpart, List<ConsumerRecord<String, byte[]>> partition)
		throws Exception {
		long lastOffset = -1;
		for ( ConsumerRecord<String, byte[]> rec: partition ) {
			TrackletId target = TrackletId.fromString(rec.key());
			
			boolean removed = m_wanteds.remove(target);
			if ( getLogger().isInfoEnabled() ) {
				if ( removed ) {
					getLogger().info("received tracklet-association: tracklet={} => remove hold", target);
				}
				else {
					getLogger().info("received tracklet-association: tracklet={}", target);
				}
			}
			lastOffset = Math.max(lastOffset, rec.offset());
		}
		
		return (lastOffset >= 0) ? ProcessResult.of(tpart, lastOffset+1, false) : ProcessResult.NULL;
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		return 0;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	@Override
	public String toString() {
		String wantedListStr = FStream.from(m_wanteds).map(TrackletId::toString).join(",", "{", "}");
		
		Set<TrackletId> cachedKeys = m_cache.getCachedAssociationKeys();
		String cachedListStr = FStream.from(cachedKeys).map(TrackletId::toString).join(",");
		if ( cachedKeys.size() > 3 ) {
			cachedListStr = String.format("{%s,...}", FStream.from(cachedKeys).take(3).toList());
		}
		else {
			cachedListStr = String.format("{%s}", cachedKeys);
		}
		
		return String.format("hold=%s, cache=%s", wantedListStr, cachedListStr);
	}
}
