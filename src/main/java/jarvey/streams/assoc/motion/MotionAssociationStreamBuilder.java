package jarvey.streams.assoc.motion;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;

import com.google.common.collect.Sets;

import utils.LoggerNameBuilder;
import utils.func.Either;
import utils.func.Funcs;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

import jarvey.streams.assoc.Association;
import jarvey.streams.assoc.AssociationCollection;
import jarvey.streams.assoc.AssociationStatusRegistry;
import jarvey.streams.assoc.AssociationStore;
import jarvey.streams.assoc.BinaryAssociation;
import jarvey.streams.assoc.BinaryAssociationCollection;
import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.assoc.MCMOTConfig.MotionConfigs;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MotionAssociationStreamBuilder {
	private static final Logger s_logger = LoggerNameBuilder.from(MotionAssociationStreamBuilder.class)
																.dropSuffix(1)
																.getLogger();
	private static final Duration PURGE_TIMEOUT = Duration.ofMinutes(5);
	private final MotionConfigs m_configs;
	
	private final OverlapAreaRegistry m_areaRegistry;
	private final BinaryAssociationCollection m_binaryAssociations = new BinaryAssociationCollection(true);
	private final AssociationCollection m_associations;
	private final AssociationCollection m_finalAssociations;
	private final JdbcProcessor m_jdbc;
	private final AssociationStore m_assocStore;
	private final Set<TrackletId> m_closedTracklets = Sets.newHashSet();
	private final AssociationStatusRegistry m_statusRegistry = new AssociationStatusRegistry();
	
	public static final TrackletId TRK_ID = new TrackletId("etri:02", "5");
	
	public MotionAssociationStreamBuilder(MCMOTConfig configs,
											AssociationCollection associations,
											AssociationCollection finalAssociations) {
		m_configs = configs.getMotionConfigs();
		m_areaRegistry = m_configs.getOverlapAreaRegistry();
		m_jdbc = configs.getJdbcProcessor();
		m_assocStore = new AssociationStore(m_jdbc);
		m_associations = associations;
		m_finalAssociations = finalAssociations;
	}
	
	public KStream<String,String> build(KStream<String,NodeTrack> nodeTracks) {
		FinalAssociationSelector finalAssocSelector
			= new FinalAssociationSelector(m_binaryAssociations, m_associations, m_closedTracklets);
		
		return nodeTracks
			// 카메라로부터 일정 거리 이내의 track 정보만 활용한다.
			// 키 값도 node-id에서 overlap-area id로 변경시킨다.
			.filter(this::withAreaDistance)
			// key를 node id에서 overlap-area의 식별자로 변환시킨다.
			.selectKey((nodeId, ntrack) -> m_areaRegistry.findByNodeId(nodeId).get().getId())
			// tracklet을 등록시켜 상태를 추적한다.
			.peek((k,v) -> m_statusRegistry.register(v.getTrackletId()))
			// 일정 기간만큼씩 끊어서 track들 사이의 거리를 통해 binary association을 계산한다.
			.process(() -> createBinaryAssociator(), Named.as("motion-binary-association"))
			// Association 리스트에 생성된 binary association을 등록시키고, 종료시킨다.
			// 'TrackletDeleted' 이벤트의 경우에는 다음으로 넘긴다.
			.flatMapValues(either -> processIfBinaryAssociation(either))
			.flatMapValues(deleted -> handleTrackDeleted(deleted, finalAssocSelector))
			.map((nodeId,kv) -> KeyValue.pair(kv.key.toString(), kv.value.getId()));
	}
	
	private MotionBinaryTrackletAssociator createBinaryAssociator() {
		return new MotionBinaryTrackletAssociator(m_configs, m_binaryAssociations);
	}
	
	private List<TrackletDeleted> processIfBinaryAssociation(Either<BinaryAssociation, TrackletDeleted> either) {
		if ( either.isLeft() ) {
			m_associations.add(either.getLeft());
			return Collections.emptyList();
		}
		else {
			return Collections.singletonList(either.getRight());
		}
	}
	
	private boolean withAreaDistance(String nodeId, NodeTrack track) {
		// Overlap area에서 검출된 NodeTrack이 아니면 무시한다.
		return m_areaRegistry.findByNodeId(nodeId)
							.map(area -> {
								if ( track.isDeleted() ) {
									return true;
								}
								else {
									return track.getDistance() <= area.getDistanceThreshold(track.getNodeId());
								}
							})
							.getOrElse(false);
	}
	
	private List<KeyValue<TrackletId,Association>>
	handleTrackDeleted(TrackletDeleted deleted, FinalAssociationSelector selector) {
		TrackletId trkId = deleted.getTrackletId();
	
		m_closedTracklets.add(trkId);
		List<Association> finalAssocList = FStream.from(selector.apply(deleted))
													.flatMap(a -> FStream.from(m_finalAssociations.add(a)))
													.peek(a -> m_assocStore.addAssociation(a))
													.toList();
		List<KeyValue<TrackletId,Association>> ret
			= FStream.from(finalAssocList)
					.flatMap(assoc -> FStream.from(assoc.getTracklets())
											.map(t -> KeyValue.pair(t, assoc)))
					.distinct(kv -> kv.key)
					.peek(kv -> m_statusRegistry.setAssigned(kv.key, kv.value))
					.toList();
		
		if ( !m_statusRegistry.isAssigned(trkId)
			&& !Funcs.exists(m_associations, assoc -> assoc.containsTracklet(trkId)) ) {
			Association singleAssoc = produceSingleAssociation(trkId, deleted.getTimestamp());
			return Collections.singletonList(KeyValue.pair(trkId, singleAssoc));
		}
		else {
			ret.forEach(kv -> {
				m_statusRegistry.unregister(kv.key);
				m_closedTracklets.remove(kv.key);
			});
			
			// m_closedTracklets에 등록되었지만, 해당 tracklet이 m_associations에 존재하지 않다면
			// association에 실패한 tracklet으로 간주하여 single association으로 생성한다.
			List<TrackletId> danglings
				= FStream.from(m_closedTracklets)
							.filter(ct -> !FStream.from(m_associations).exists(a -> a.containsTracklet(ct)))
							.toList();
			if ( danglings.size() > 0 ) {
				FStream.from(danglings)
						.forEach(d -> {
							Association single = produceSingleAssociation(d, 0);
							ret.add(KeyValue.pair(d, single));
						});
			}
			m_finalAssociations.removeIf(assoc -> Duration.ofMillis(deleted.getTimestamp() - assoc.getTimestamp())
															.compareTo(PURGE_TIMEOUT) > 0);
			
			return ret;
		}
	}
	
	private Association produceSingleAssociation(TrackletId trkId, long ts) {
		Association singleAssoc = Association.singleton(trkId, ts);
//		m_finalAssociations.add(singleAssoc);
		m_assocStore.addAssociation(singleAssoc);
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("generate singleton association: {}", trkId);
		}
		m_statusRegistry.unregister(trkId);
		m_closedTracklets.remove(trkId);
		
		return singleAssoc;
	}
}
