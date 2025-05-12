package jarvey.streams.assoc;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.LoggerSettable;
import utils.UnitUtils;
import utils.func.FOption;
import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.streams.assoc.GlobalTrack.State;
import jarvey.streams.assoc.MCMOTConfig.OutputConfigs;
import jarvey.streams.model.JarveySerdes;
import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.optor.HoppingWindowManager;
import jarvey.streams.optor.WindowBasedEventCollector;
import jarvey.streams.optor.Windowed;
import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;
import jarvey.streams.serialization.json.GsonUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class GlobalTrackGenerator implements KafkaConsumerRecordProcessor<String,byte[]>, LoggerSettable {
	private static final Logger s_logger = LoggerFactory.getLogger(GlobalTrackGenerator.class);
			
	private static final long WARN_ASSOC_DELAY = UnitUtils.parseDurationMillis("1m");
	private static final long MAX_ASSOC_DELAY = UnitUtils.parseDurationMillis("5m");
	
	private final Duration m_maxAssociationDelay;
	private final ReactiveAssociationCache m_cache;
	private final WindowBasedEventCollector<PendingNodeTrack> m_collector;
	private final List<Windowed<List<PendingNodeTrack>>> m_pendingWindows = Lists.newArrayList();
	private final Map<String,Set<TrackletId>> m_runningTrackletsMap = Maps.newHashMap();
	
	private final KafkaProducer<String,byte[]> m_producer;
	private final String m_outputTopic;
	private final Deserializer<NodeTrack> m_nodeTrackDeser;
	private final Serializer<GlobalTrack> m_gtrackSer;

	private Logger m_logger = s_logger;
	private int m_lastWarnLevel = 0;
	
	private class PendingNodeTrack implements Timestamped {
		final NodeTrack m_track;
		final TopicPartition m_tpart;
		final long m_topicOffset;
		Association m_assoc;
		
		PendingNodeTrack(NodeTrack track, TopicPartition tpart, long topicOffset) {
			m_track = track;
			m_tpart = tpart;
			m_topicOffset = topicOffset;
		}
		
		String getAssociationId() {
			return (m_assoc != null) ? m_assoc.getId() : null;
		}

		@Override
		public long getTimestamp() {
			return m_track.getTimestamp();
		}
		
		@Override
		public String toString() {
			String assocStr = m_assoc != null ? m_assoc.getId() : "?";
			return String.format("%s{%s}: T(%d:%d) #(%d)", m_track.getKey(), assocStr,
									m_track.getFrameIndex(), m_track.getTimestamp(), m_topicOffset);
		}
	};
	
	public GlobalTrackGenerator(MCMOTConfig configs, ReactiveAssociationCache cache,
								KafkaProducer<String,byte[]> producer) {
		OutputConfigs outConfigs = configs.getOutputConfigs();

		m_maxAssociationDelay = outConfigs.getMaxAssociationDelay();
		m_producer = producer;
		m_outputTopic = configs.getGlobalTracksTopic();
		m_nodeTrackDeser = JarveySerdes.NodeTrack().deserializer();
		m_gtrackSer = GsonUtils.getSerde(GlobalTrack.class).serializer();
		
		m_cache = cache;
		HoppingWindowManager winMgr = HoppingWindowManager.ofWindowSize(outConfigs.getInterval())
															.graceTime(outConfigs.getGraceTime());
		m_collector = new WindowBasedEventCollector<>(winMgr);
	}
	
	@Override
	public void close() throws Exception {
		m_pendingWindows.addAll(m_collector.close());
		
		processPendingWindows();
	}

	@Override
	public ProcessResult process(TopicPartition tpart, ConsumerRecord<String, byte[]> record) {
		NodeTrack track = m_nodeTrackDeser.deserialize(record.topic(), record.value());
		PendingNodeTrack pending = new PendingNodeTrack(track, tpart, record.offset());
		m_pendingWindows.addAll(m_collector.collect(pending));
		
		return processPendingWindows();
	}
	
	private ProcessResult processPendingWindows() {
		ProcessResult result = ProcessResult.empty();
		while ( m_pendingWindows.size() > 0 ) {
			Windowed<List<PendingNodeTrack>> window = m_pendingWindows.get(0);
			if ( !attachAssociations(window) ) {
				return ProcessResult.NULL;
			}
			m_pendingWindows.remove(0);
			
			FStream.from(window.value())
					.tagKey(PendingNodeTrack::getAssociationId)
					.groupByKey()
					.fstream()
					.forEach((aid, batch) -> processAssociation(aid, batch).forEach(this::publish));
			
			FStream.from(window.value())
					.tagKey(pt -> pt.m_tpart)
					.groupByKey()
					.fstream()
					.mapValue(lst -> FStream.from(lst).mapToLong(ptrack -> ptrack.m_topicOffset).max().get())
					.forEach((tp, of) -> result.add(tp, of));
		}
		
		return result;
	}
	
	@Override
	public ProcessResult timeElapsed(TopicPartition tpart, long expectedTs) {
		m_pendingWindows.addAll(m_collector.progress(expectedTs));
		
		return processPendingWindows();
	}

	@Override
	public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
		return m_nodeTrackDeser.deserialize(record.topic(), record.value()).getTimestamp();
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	private List<GlobalTrack> processAssociation(String assocId, List<PendingNodeTrack> supports) {
		// 'delete' track을 먼저 따로 뽑는다.
		List<PendingNodeTrack> deleteds = Funcs.filter(supports, pt -> pt.m_track.isDeleted());
		
		List<GlobalTrack> gtracks = Lists.newArrayList();
		if ( assocId != null ) {
			List<LocalTrack> ltracks = FStream.from(supports)
												.filterNot(pt -> pt.m_track.isDeleted())
												.map(pt -> LocalTrack.from(pt.m_track))
												.toList();
			if ( ltracks.size() > 0 ) {
				Association assoc = supports.get(0).m_assoc;
				gtracks.add(GlobalTrack.from(assoc, ltracks));
			}
		}
		else {
			FStream.from(supports)
					.tagKey(pt -> pt.m_track.getTrackletId())
					.groupByKey()
					.fstream()
					.map((tid, ptracks) -> average(null, ptracks))
					.forEach(gtracks::add);
		}
		
		// delete event의 경우는 association 별로 소속 tracklet이 모두 delete되는
		// 경우에만 delete global track을 추가하되, 이때도 association id를 사용한다.
		if ( deleteds.size() > 0 ) {
			Association assoc = deleteds.get(0).m_assoc;
			handleTrackletDeleted(assoc, deleteds).forEach(gtracks::add);
		}
		
		return gtracks;
	}
	
	private boolean attachAssociations(Windowed<List<PendingNodeTrack>> window) {
		for ( PendingNodeTrack ptrack: window.value() ) {
			if ( ptrack.m_assoc == null ) {
				boolean attachFailed = m_cache.getAssociation(ptrack.m_track.getTrackletId())
												.orElse(() -> handleUnknownAssociation(ptrack.m_track))
												.ifPresent(a -> ptrack.m_assoc = a)
												.isAbsent();
				if ( attachFailed ) {
					return false;
				}
			}
		}
		
		return true;
	}
	
	private GlobalTrack average(Association assoc, List<PendingNodeTrack> ptracks) {
		List<LocalTrack> ltracks = Funcs.map(ptracks, pt -> LocalTrack.from(pt.m_track));
		return GlobalTrack.from(assoc, ltracks);
	}
	
	private void publish(GlobalTrack gtrack) {
		if ( getLogger().isDebugEnabled() ) {
			getLogger().debug("publish a global-track: {}", gtrack);
		}
		
		byte[] bytes = m_gtrackSer.serialize(m_outputTopic, gtrack);
		m_producer.send(new ProducerRecord<>(m_outputTopic, gtrack.getKey(), bytes));
	}
	
	private FOption<Association> handleUnknownAssociation(NodeTrack track) {
		TrackletId trkId = track.getTrackletId();
		long ageMillis = m_collector.getLastTimestamp() - track.getTimestamp();
		
		int warnLevel = (int)(ageMillis / WARN_ASSOC_DELAY);
		if ( warnLevel > m_lastWarnLevel ) {
			m_lastWarnLevel = warnLevel;
			getLogger().warn("old unassociated tracklet: {}, delay={}, {}",
								trkId, UnitUtils.toSecondString(ageMillis), m_cache);
		}
		
		if ( Duration.ofMillis(ageMillis).compareTo(m_maxAssociationDelay) > 0 ) { 
			// 일정 시간이 경과한 이후에도 주어진 tracklet-id에 해당하는 association이 없는
			// 경우에는 그 tracklet이 다른 tracklet과 association되지 않은 것으로 간주하고
			// 단일 tracklet으로 구성된 association을 임시로 만들어 global track을 생성한다.
			Association tmpAssoc = Association.singleton(trkId, track.getFirstTimestamp());
			m_cache.putAssociation(trkId, tmpAssoc);

			getLogger().warn("generate a single association to remove holder {}", trkId);
			
			return FOption.of(tmpAssoc);
		}
		else {
			return FOption.empty();
		}
	}
	
	private List<GlobalTrack> handleTrackletDeleted(Association assoc, List<PendingNodeTrack> deleteds) {
		if ( assoc == null ) {
			return Funcs.map(deleteds, pt -> GlobalTrack.from(LocalTrack.from(pt.m_track), assoc));
		}
		
		// Association에 참여하는 tracklet들의 delete 여부를 association별로 관리한다.
		// tracklet의 종료하는 경우 'm_runningTrackletsMap'에서 제거하고
		// Association에 속하는 모든 tracklet들이 모두 종료될 때 association id를 사용한
		// Deleted 이벤트를 발생시킨다.
		Set<TrackletId> participants = m_runningTrackletsMap.computeIfAbsent(assoc.getId(),
																		k -> Sets.newHashSet(assoc.getTracklets()));
		deleteds.forEach(pt -> participants.remove(pt.m_track.getTrackletId()));
		if ( participants.isEmpty() ) {
			// Association에 참여하는 모든 tracklet들이 종료된 경우.
			m_runningTrackletsMap.remove(assoc.getId());
			
			// Association의 id와 소속 tracklet들 중에서 가장 마지막으로 종료된 tracklet의
			// timestamp를 이용하여 deleted 이벤트를 생성한다.
			LocalTrack last = Funcs.max(deleteds, pt -> pt.m_track.getTimestamp())
									.map(pt -> LocalTrack.from(pt.m_track)).get();
			GlobalTrack gtrack = new GlobalTrack(assoc.getId(), State.DELETED,
												null, null, assoc.getFirstTimestamp(), last.getTimestamp());
			return Collections.singletonList(gtrack);
		}
		else {
			return Collections.emptyList();
		}
	}
	
	private long findMaxOffset(List<PendingNodeTrack> ptracks) {
		return FStream.from(ptracks).mapToLong(pt -> pt.m_topicOffset).max().get();
	}
}
