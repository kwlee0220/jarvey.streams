package jarvey.streams.assoc.feature;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;

import utils.jdbc.JdbcProcessor;

import jarvey.streams.KafkaParameters;
import jarvey.streams.assoc.Association;
import jarvey.streams.assoc.AssociationStatusRegistry;
import jarvey.streams.assoc.AssociationStatusRegistry.Status;
import jarvey.streams.assoc.AssociationStore;
import jarvey.streams.assoc.MCMOTConfig;
import jarvey.streams.assoc.MCMOTConfig.FeatureConfigs;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.node.TrackFeature;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class FeatureAssociationStreamBuilder {
	private static final Duration PURGE_TIMEOUT = Duration.ofMinutes(5);
	
	private final FeatureConfigs m_configs;
	private final JdbcProcessor m_jdbc;
	private final Properties m_consumerProps;
	private final AssociationStatusRegistry m_statusRegistry = new AssociationStatusRegistry();
	
	private static final TrackletId TRK_ID = new TrackletId("etri:01", "17");
	
	public FeatureAssociationStreamBuilder(MCMOTConfig configs) {
		m_configs = configs.getFeatureConfigs();
		m_jdbc = configs.getJdbcProcessor();
		
		KafkaParameters params = new KafkaParameters();
		params.setBootstrapServers(configs.getKafkaBootstrapServers());
		m_consumerProps = params.toConsumerProperties();
	}
	
	public KStream<String,String> build(KStream<String,NodeTrack> nodeTracks,
										KStream<String,TrackFeature> trackFeatures) {
		FeatureBinaryTrackletAssociator binaryAssociator
			= new FeatureBinaryTrackletAssociator(m_configs.getMCMOTNetwork(), m_jdbc,
													m_consumerProps, m_configs.getTopPercent());
		
		AssociationStore assocStore = new AssociationStore(m_jdbc);

		// TrackFeature 이벤트를 이용해 association을 시도한다.
		trackFeatures
			= trackFeatures
				.filter((nodeId, tfeat) -> m_configs.getListeningNodes().contains(nodeId))
				.peek((k,v) -> m_statusRegistry.register(v.getTrackletId()))
				// 이미 assign이 완료된 TrackFeature 이벤트는 더 이상 처리할 필요가 없다.
				.filterNot((k, v) -> !v.isDeleted() && m_statusRegistry.isAssigned(v.getTrackletId()));
//		if ( m_configs.getMatchDelay().toMillis() > 0) {
//			delayedFeatures = delayedFeatures
//				// node-track-index와 track-features-index 테이블에 필요한 데이터가 먼저 준비되게하기 위해
//				// 일부러 일정 기간동안 feature 처리를 지연시킨다.
//				.process(() -> new DelayEvents<>(m_configs.getMatchDelay()), Named.as("delay-track-features"));
//		}
		
		KStream<String,String> trackletAssociateds
			= trackFeatures
				.flatMapValues(binaryAssociator, Named.as("binary-feature-association"))
				// Matching score가 일정 값 이하인 경우에는 무시한다.
				.filter((nodeId,ba) -> ba.getScore() >= m_configs.getMinBinaryAssociationScore())
				.map((k, ba) -> {
					TrackletId trkId = ba.getFollower().getTrackletId();
					
					// 체결된 binary association을 이용해서 association을 만들고,
					// 이를 AssociationStore에 저장한다.
					// 저장 결과 확장된 association이 반환되기 때문에 이를 최종 결과로 사용한다.
					Association assoc = Association.from(Collections.singletonList(ba));
					Association extended = assocStore.addAssociation(assoc);
					if ( assoc.size() != extended.size() ) {
						if ( binaryAssociator.getLogger().isInfoEnabled() ) {
							binaryAssociator.getLogger().info("Association extended: {}", extended);
						}
					}
					m_statusRegistry.setAssigned(trkId, extended);
					return KeyValue.pair(trkId.toString(), extended.getId());
				});
		KStream<String,String> trackletUnassigneds
			= trackFeatures
				.filter((k,tfeat) -> tfeat.isDeleted())
				.flatMap((k,tfeat) -> {
					TrackletId trkId = tfeat.getTrackletId();		
					Status status = m_statusRegistry.unregister(trkId).get();
					if ( !status.isAssociated() ) {
						// delete 이벤트가 도착할 때까지도 association되지 못한 경우
						Association singleAssoc = Association.singleton(trkId, tfeat.getTimestamp());
						assocStore.addAssociation(singleAssoc);
						
						Logger logger = binaryAssociator.getLogger();
						if ( logger.isDebugEnabled() ) {
							logger.debug("singleton association: {}", trkId);
						}
						return Collections.singletonList(KeyValue.pair(trkId.toString(), trkId.toString()));
					}
					else {
						return Collections.emptyList();
					}
				});
		return trackletAssociateds.merge(trackletUnassigneds);
	}
}
