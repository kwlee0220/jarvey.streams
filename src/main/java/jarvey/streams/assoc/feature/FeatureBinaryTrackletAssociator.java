package jarvey.streams.assoc.feature;

import static utils.Utilities.checkArgument;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;

import utils.LoggerNameBuilder;
import utils.LoggerSettable;
import utils.func.FOption;
import utils.func.Funcs;
import utils.func.Tuple;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

import jarvey.streams.assoc.BinaryAssociation;
import jarvey.streams.assoc.feature.FeatureBinaryTrackletAssociator.MatchingSession.State;
import jarvey.streams.assoc.feature.MCMOTNetwork.IncomingLink;
import jarvey.streams.assoc.feature.MCMOTNetwork.ListeningNode;
import jarvey.streams.model.Range;
import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackFeatureSerde;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrackletIndex;
import jarvey.streams.node.NodeTrackletUpdateLogs;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.updatelog.KeyedUpdateIndex;
import jarvey.streams.updatelog.KeyedUpdateLogs;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FeatureBinaryTrackletAssociator
		implements ValueMapper<TrackFeature, Iterable<BinaryAssociation>>, LoggerSettable {
	private static final Logger s_logger = LoggerNameBuilder.from(FeatureBinaryTrackletAssociator.class)
																.dropSuffix(1)
																.getLogger();
	private static final Logger LOGGER_CANDIDATE = LoggerNameBuilder.plus(s_logger, "candidate");
	private static final Logger LOGGER_DIST = LoggerNameBuilder.plus(s_logger, "dist");
	
	private static final int DECISION_COUNT = 5;

	private final MCMOTNetwork m_network;
	private final KeyedUpdateLogs<TrackFeature> m_featureIndexes;
	private final NodeTrackletUpdateLogs m_trackletIndexes;
	private final double m_topPercent;
	private Logger m_logger = s_logger;
	
	private Map<TrackletId, TrackletFeatureMatrix> m_trackletFeaturesCache = Maps.newHashMap();
	
	// Association이 진행 중인 tracklet에 대한 정보를 관리한다.
	// Association하려는 차량 하나에 대해 MatchingSession 객체가 생성된다.
	private final Map<TrackletId, MatchingSession> m_sessions = Maps.newHashMap();
	
	// 각 node 별로 association 대상 중인 tracklet들을 관리한다.
	// Node별로 관리 중인 ready 상태의 tracklet들을 start timestamp별로 순서대로 관리된다.
	private final Map<String,PriorityQueue<MatchingSession>> m_pendingQueues = Maps.newHashMap();
	
	// 하나의 차량에 대한 association 대상들 중에서 이미 이전 차량에 의해 association된 경우를
	// 알기 위해 이미 association된 차량(tracklet) 정보를 관리한다.
	// 본 container는 일정 수의 tracklet이 저장되면 오래된 tracklet부터 자동적으로 삭제된다.
	private final AutoPurgedBinaryAssociations m_matches = new AutoPurgedBinaryAssociations();
	
	private static final TrackletId TRK_ID = new TrackletId("etri:06", "2");

	public FeatureBinaryTrackletAssociator(MCMOTNetwork network, JdbcProcessor jdbc,
											Properties consumerProps, double topPercent) {
		setLogger(s_logger);
		
		m_network = network;
		
		Deserializer<TrackFeature> featureDeser = TrackFeatureSerde.getInstance().deserializer();
		m_featureIndexes = new KeyedUpdateLogs<>(jdbc, "track_features_index", consumerProps,
												"track-features", featureDeser);
		m_trackletIndexes = new NodeTrackletUpdateLogs(jdbc, "node_tracks_index",
														consumerProps, "node-tracks");
		m_topPercent = topPercent;
	}
	
	@Override
	public Iterable<BinaryAssociation> apply(TrackFeature tfeat) {
		TrackletId trkId = tfeat.getTrackletId();
		
		if ( tfeat.isDeleted() ) {
			FOption<BinaryAssociation> bassoc = FOption.empty();
			MatchingSession session = m_sessions.get(tfeat.getTrackletId());
			if ( session != null ) {
				switch ( session.getState() ) {
					case IDENTIFYING_ENTER_ZONE:
					case IDENTIFYING_MATCHING_CANDIDATES:
						if ( getLogger().isInfoEnabled() ) {
							getLogger().info("Tracklet {} terminated while waiting for EnterZone",
											session.getTrackletId());
						}
						break;
					case GATHER_FEATURES:
						// Association 대상 물체에 대한 delete event가 도착한 경우에는
						// association에 필요한 feature가 모두 준비되지 않은 경우에도
						// 지금까지 모은 feature들 만 사용해서 association을 시도한다.
						// Association을 위한 감시 tracklet의 enter-zone을 확인한다.
						if ( onEnterZoneIdentified(session) != State.READY ) {
							break;
						}
					case READY:
						bassoc = associate(session);
						break;
					default:
						break;
				}
			}
			tearDownSession(trkId);
			
			return bassoc.toList();
		}
		
		// 주어진 feature에 해당하는 MatchingSession을 찾아 새 feature를 추가한다.
		final String nodeId = trkId.getNodeId();
		MatchingSession session = getOrCreateSession(trkId);
		if ( session.getState() == State.DISABLED ) {
			return Collections.emptyList();
		}
		session.addTrackFeature(tfeat);
		
		switch ( session.getState() ) {
			case GATHER_FEATURES:
				if ( session.getFeatureCount() < DECISION_COUNT ) {
					return Collections.emptyList();
				}
				else {
					session.setState(State.IDENTIFYING_ENTER_ZONE);
				}
			case IDENTIFYING_ENTER_ZONE:
				State state = onEnterZoneIdentified(session);
				if ( state != State.READY ) {
					return Collections.emptyList();
				}
			case READY:
				List<BinaryAssociation> rets = Lists.newArrayList();
				
				// 동일 노드에 대해서는 들어온 차량 순서대로 association 수행한다.
				PriorityQueue<MatchingSession> queue = m_pendingQueues.get(nodeId);
				MatchingSession head;
				while ( (head = queue.peek()) != null && head.getState() == State.READY ) {
					associate(session).forEach(rets::add);
					queue.remove(head);
					head.setState(State.DISABLED);
				}
				
				return rets;
			default:
				throw new AssertionError();
		}
	}
	
	private MatchingSession getOrCreateSession(TrackletId trkId) {
		return  m_sessions.computeIfAbsent(trkId, tk -> {
			MatchingSession s = new MatchingSession(tk);
			
			PriorityQueue<MatchingSession> queue = m_pendingQueues.get(tk.getNodeId());
			if ( queue == null ) {
				queue = new PriorityQueue<>((s1,s2) -> Long.compare(s2.m_startTs, s1.m_startTs));
				m_pendingQueues.put(tk.getNodeId(), queue);
			}
			queue.add(s);
			
			return s;
		});
	}
	
	private State onEnterZoneIdentified(MatchingSession session) {
		// Association을 위한 감시 tracklet의 enter-zone을 확인한다.
		if ( !identifyEnterzone(session) ) {
			return session.getState();
		}
		session.setState(State.IDENTIFYING_MATCHING_CANDIDATES);
		
		// 현 tracklet과 matching 가능한 타 node에서 진출한 tracklet들을 결정하고,
		// 해당 tracklet들이 생성한 tracklet feature들을 수집한다.
		List<NodeTrackletIndex> candidates = identifyMatchingCandidates(session);
		if ( candidates.isEmpty() ) {
			// matching 가능한 타 node tracklet이 없는 경우는 matching을 종료시킨다.
			session.setState(State.DISABLED);
			m_pendingQueues.get(session.getNodeId()).remove(session);
			if ( getLogger().isInfoEnabled() ) {
				getLogger().info("{}: -> No Candidates", session.getTrackletId());
			}
			
			return State.DISABLED;
		}
		
		FStream.from(candidates)
				.flatMapFOption(idx -> getTrackletFeatureMatrix(idx.getTrackletId()))
				.forEach(matrix -> {
					TrackletId tid = matrix.getTrackletId();
					m_trackletFeaturesCache.put(tid, matrix);
					session.m_assocCandidates.put(tid, matrix);
				});
		session.setState(State.READY);
		
		return State.READY;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	private static final class TaggedAssociation implements Timestamped {
		private final BinaryAssociation m_ba;
		private final TrackletId m_peerTrkId;
		private final long m_ts;
		
		TaggedAssociation(BinaryAssociation ba, TrackletId peerId, long ts) {
			m_ba = ba;
			m_peerTrkId = peerId;
			m_ts = ts;
		}
		
		public BinaryAssociation getAssociation() {
			return m_ba;
		}
		
		public TrackletId getPeerId() {
			return m_peerTrkId;
		}
		
		public double getScore() {
			return m_ba.getScore();
		}

		@Override
		public long getTimestamp() {
			return m_ts;
		}

		@Override
		public String toString() {
			return String.format("%s<->%s:%.3f", m_ba.getOther(m_peerTrkId), m_peerTrkId, m_ba.getScore());
		}
	};
	private static final class AssociationGroup {
		private final String m_nodeId;
		private final List<TaggedAssociation> m_assocList;
		private final double m_bestScore;
		
		AssociationGroup(String nodeId, List<TaggedAssociation> assocList) {
			checkArgument(assocList != null && assocList.size() > 0, "invalid association list");
			
			m_nodeId = nodeId;
			m_assocList = assocList;
			m_assocList.sort((tba1, tba2) -> Long.compare(tba1.getTimestamp(), tba2.getTimestamp()));
			m_bestScore = FStream.from(assocList).map(TaggedAssociation::getScore).max().get();
		}
		
		public int size() {
			return m_assocList.size();
		}
		
		public TaggedAssociation selectBestAssociation() {
			if ( m_assocList.size() == 1 ) {
				return m_assocList.get(0);
			}
			else {
				return FStream.from(m_assocList)
						.sort(TaggedAssociation::getTimestamp)
						.zipWith(newSequenceRatioStream())
						.map(t -> Tuple.of(t._1.getScore() * t._2, t._1))
						.max(t -> t._1)
						.map(t -> t._2)
						.get();
			}
		}
		
		@Override
		public String toString() {
			if ( m_assocList.size() > 1 ) {
				String basStr = FStream.from(m_assocList)
										.zipWith(newSequenceRatioStream())
										.map(t -> Tuple.of(t._1.getScore() * t._2, t._1.m_ba))
										.map(t -> String.format("%s(%.3f->%.3f)", getTrackId(t._2), t._2.getScore(), t._1))
										.join(", ");
				return String.format("%s:[%s]", m_nodeId, basStr);
			}
			else {
				TaggedAssociation tagged = m_assocList.get(0);
				return String.format("%s:[%s(%.3f)]", m_nodeId, tagged.getPeerId(), tagged.getScore());
			}
		}
		
		private String getTrackId(BinaryAssociation ba) {
			return ba.getLeftTrackletId().getNodeId().equals(m_nodeId)
					? ba.getLeftTrackletId().getTrackId() : ba.getRightTrackletId().getTrackId();
		}
	};

	private FOption<BinaryAssociation> associate(MatchingSession session) {
		return getBestAssociation(session);
	}
	private FOption<BinaryAssociation> getBestAssociation(MatchingSession session) {
		TrackletId thisTrkId = session.getTrackletId();
		
		List<AssociationGroup> assocGroups =
			FStream.from(session.m_assocCandidates.values())
					.filter(c -> !m_matches.containsKey(c.getTrackletId()))
					.flatMapFOption(c -> {
						return associate(session, c)
								.map(ba -> {
									TrackletId peerId = ba.getOther(thisTrkId);
									TaggedAssociation tba = new TaggedAssociation(ba, peerId, c.getExitTimestamp());
									if ( getLogger().isDebugEnabled() ) {
										String msg = String.format("candidate association: %s<->%s (%.2f, %.1fs)",
																	session.getTrackletId(), tba.getPeerId(),
																	tba.getScore(),
																	(session.m_startTs-tba.getTimestamp())/1000f);
										getLogger().debug(msg);
									}
									return tba;
								});
					})
					.groupByKey(tba -> tba.getPeerId().getNodeId())
					.stream()
					.map(kv -> new AssociationGroup(kv.key(), kv.value()))
					.toList();
		
		return 	FStream.from(assocGroups)
						.max(grp -> grp.m_bestScore)
						.map(grp -> grp.selectBestAssociation())
						.ifPresent(tba -> {
							m_matches.put(tba.getPeerId(), tba.getAssociation());
							
							if ( getLogger().isInfoEnabled() ) {
								String grpStr = FStream.from(assocGroups)
														.map(grp -> grp.toString())
														.join(", ");
								String msg = String.format("%s: %s -> %s(%.3f)",
															session.getTrackletId(), grpStr,
															tba.getPeerId(), tba.getScore());
								getLogger().info(msg);
							}
						})
						.ifAbsent(() -> {
							if ( getLogger().isInfoEnabled() ) {
								String candIdsStr = FStream.from(session.m_assocCandidates.values())
															.map(TrackletFeatureMatrix::getTrackletId)
															.join(", ");
								String msg = String.format("%s: {} -> None (candidates: %s), source={}",
															session.getTrackletId(), candIdsStr, assocGroups);
								getLogger().info(msg);
							}
						})
						.map(TaggedAssociation::getAssociation);
	}
	
	private boolean identifyEnterzone(MatchingSession session) {
		// NodeTrackletIndex에서 주어진 tracklet에 해당하는 인덱스를 검색하여
		// 카메라 시야 진입 영역 정보를 검색한다.
		// TrackFeature의 index 정보와 NodeTrackIndex 정보가 각각 독립적으로 build되기
		// 때문에, 한 tracklet의 TracFeature를 처리할 때, 해당 NodeTrackIndex가 아직 존재하지
		// 않을 수 있기 때문에 일시적으로 NodeTrackIndex가 없는 경우도 고려해야 한다.
		
		TrackletId trkId = session.m_trkId;
		NodeTrackletIndex trackletIndex = m_trackletIndexes.getIndex(trkId);
		if ( trackletIndex != null ) {
			session.setFirstTimestamp(trackletIndex.getFirstTimestamp());
			if ( trackletIndex.getEnterZone() != null ) {
				session.setEnterZone(trackletIndex.getEnterZone());
				if ( getLogger().isDebugEnabled() ) {
					getLogger().debug("Tracklet[{}] enters Zone[{}]", trkId, session.getEnterZone());
				}
				return true;
			}
			else {
				// 본 tracklet의 index 정보는 검색되었으나, 아직 enter-zone이 식별되지 않은 경우.
				return false;
			}
		}
		else {
			if ( getLogger().isDebugEnabled() ) {
				getLogger().debug("Target tracklet's index has not been identified: id={}", trkId);
			}
			return false;
		}
	}
	
	private List<NodeTrackletIndex> findNodeTracksFromIncomingLink(MatchingSession session, IncomingLink link) {
		// FIXME: 디버깅 후 삭제
//		if ( session.getTrackletId().equals(TRK_ID) ) {
//			System.out.print("");
//		}
		
		// 이전 노드에서의 candidate tracklet의 예상 exit 시간 구간을 계산하여
		// 해당 구간에 exit한 tracklet들을 후보들의 NodeTrackletIndex 정보는 읽어온다.
		Range<Duration> transRange = link.getTransitionTimeRange();
		long minTs = session.m_startTs - transRange.max().toMillis();
		long maxTs = session.m_startTs - transRange.min().toMillis();
		String whereClause
			= String.format("node = '%s' and exit_zone='%s' and last_ts between %d and %d",
							link.getExitNode(), link.getExitZone(), minTs, maxTs);
		return m_trackletIndexes.findIndexes(whereClause);
	}
	
	private static String toIndexString(NodeTrackletIndex idx, long startTs) {
		long transMillis = startTs - idx.getLastTimestamp();
		return String.format("%s(%.1fs)", idx.getTrackId(), transMillis/1000f);
	}
	
	private List<NodeTrackletIndex> identifyMatchingCandidates(MatchingSession session) {
		// Association 후보 tracklet을 찾기 위해 본 tracklet의 enter-zone과
		// tracklet 시작 시각을 이용하여 진입 link들을 찾는다. 
		// 
		ListeningNode listeningNode = m_network.getListeningNode(session.m_trkId.getNodeId());
		List<IncomingLink> incomingLinks = listeningNode.getIncomingLinks(session.getEnterZone());
		if ( incomingLinks.isEmpty() ) {
			return Collections.emptyList();
		}
		
		if ( getLogger().isDebugEnabled() ) {
			getLogger().debug("found the incoming links for tracklet {}[{}]: incoming-links: {}",
								session.m_trkId, session.getEnterZone(), incomingLinks);
		}
		
		// 모든 incoming link에 대해 association을 해 볼만한 tracklet들을 검색한다.
		List<NodeTrackletIndex> candidates
			= FStream.from(incomingLinks)
						.flatMap(link -> FStream.from(findNodeTracksFromIncomingLink(session, link)))
						.toList();
		if ( LOGGER_CANDIDATE.isInfoEnabled() ) {
			String msg = FStream.from(candidates)
								.groupByKey(NodeTrackletIndex::getNodeId)
								.stream()
								.map((n,idxes)
										-> String.format("[%s: %s]", n,
															FStream.from(idxes)
																	.sort(idx -> idx.getLastTimestamp())
																	.map(idx -> toIndexString(idx, session.m_startTs))
																	.join(',')))
								.join(", ");
			LOGGER_CANDIDATE.info("the candidate tracklets: {}[{}]: {{}}",
									session.m_trkId, session.getEnterZone(), msg);
		}
		
		return candidates;
	}
	
	private FOption<TrackletFeatureMatrix> getTrackletFeatureMatrix(TrackletId trkId) {
		TrackletFeatureMatrix matrix = m_trackletFeaturesCache.get(trkId);
		if ( matrix != null ) {
			if ( getLogger().isDebugEnabled() ) {
				getLogger().debug("get feature-matrix from the cache: target={}, nfeatures={}",
								matrix.getTrackletId(), matrix.getFeatureCount());
			}
			return FOption.ofNullable(matrix);
		}
		else {
			return m_featureIndexes.readIndex(trkId.toString())
									.map(idx -> loadTrackletFeatureMatrix(idx));
		}
	}
	private TrackletFeatureMatrix loadTrackletFeatureMatrix(KeyedUpdateIndex idx) {
		TrackletId trkId = TrackletId.fromString(idx.getKey());
		TrackletFeatureMatrix matrix = 
			m_featureIndexes.streamUpdatesOfIndex(idx)
							.map(kv -> kv.value)
							.filterNot(TrackFeature::isDeleted)
							.collect(new TrackletFeatureMatrix(trkId, idx.getTimestamp()),
										TrackletFeatureMatrix::addTrackFeature);
		if ( LOGGER_CANDIDATE.isDebugEnabled() ) {
			Range<Long> offsetRange = idx.getTopicOffsetRange();
			long length = offsetRange.max() - offsetRange.min() + 1;
			LOGGER_CANDIDATE.debug("load candidate features from topic: target={}, nfeatures={}, offsets={}({})",
										matrix.getTrackletId(), matrix.getFeatureCount(),
										offsetRange, length);
		}
		
		return matrix;
	}
	
	private List<TrackletId> prepareCandidateFeatures(long enterTs, IncomingLink link) {
		// 이전 노드에서의 candidate tracklet의 예상 exit 시간 구간을 계산하여
		// 해당 구간에 exit한 tracklet들을 후보들의 NodeTrackletIndex 정보는 읽어온다.
		long minTs = enterTs - link.getTransitionTimeRange().max().toMillis();
		long maxTs = enterTs - link.getTransitionTimeRange().min().toMillis();
		String whereClause
			= String.format("node = '%s' and exit_zone='%s' and last_ts between %d and %d",
							link.getExitNode(), link.getExitZone(), minTs, maxTs);
		
		List<TrackletId> candidateTrkIds
			= Funcs.map(m_trackletIndexes.findIndexes(whereClause), NodeTrackletIndex::getTrackletId);
		if ( candidateTrkIds.size() > 0 ) {
			//
			// tracklet index를 바탕으로 해당 tracklet의 feature를 뽑는다.
			// 이때, 효과적인 TrackFeature 접근을 위해, 이미 캐슁된 tracklet과
			// 캐슁되지 않는 tracklet을 따로 분리하여 처리한다.
			//
			KeyedGroups<Boolean, TrackletId> cases = FStream.from(candidateTrkIds)
															.groupByKey(k -> m_trackletFeaturesCache.get(k) != null);
			cases.switcher()
				 .ifCase(true).consume(this::shareCandidate)	// 이미 cache된 경우는 share-count만 증가.
				 .ifCase(false).consume(this::downloadCandidateFeatures);
		}
		else {
			String sqlString = String.format("select * from %s where %s",
											m_trackletIndexes.getIndexTableName(), whereClause);
			getLogger().warn("fails to find candidate tracks: sql={}", sqlString);
		}
		
		return candidateTrkIds;
	}
	
	private void downloadCandidateFeatures(List<TrackletId> trkIds) {
		List<TrackletId> missingTrkIds = FOption.getOrElse(trkIds, Collections.emptyList());
		if ( missingTrkIds.size() > 0 ) {
			List<String> missingKeys = Funcs.map(missingTrkIds, TrackletId::toString);
			for ( KeyedUpdateIndex index: m_featureIndexes.readIndexes(missingKeys).values() ) {
				if ( !index.isClosed() ) {
					if ( getLogger().isDebugEnabled() ) {
						getLogger().debug("skip for the unfinished tracklet's features: id={}", index.getKey());
					}
					continue;
				}

				TrackletId trkId = TrackletId.fromString(index.getKey());
				TrackletFeatureMatrix candidate = new TrackletFeatureMatrix(trkId, index.getTimestamp());
				m_featureIndexes.streamUpdatesOfIndex(index)
					 			.map(kv -> kv.value)
					 			.filterNot(TrackFeature::isDeleted)
					 			.forEach(candidate::addTrackFeature);
				 m_trackletFeaturesCache.put(trkId, candidate);
				if ( getLogger().isInfoEnabled() ) {
					getLogger().info("candidate tracklet's features are ready: target={}, nfeatures={}",
									index.getKey(), candidate.getFeatureCount());
				}
			}
		}
	}
	
	private void shareCandidate(List<TrackletId> trkIds) {
		trkIds.forEach(trkId -> {
			TrackletFeatureMatrix candidate = m_trackletFeaturesCache.get(trkId);
			candidate.addSharing();
		});
	}

	// NOTE: 두 tracklet의 거리를 확인할 때 사용함
//	private static final TrackletId TRK_ID1 = new TrackletId("etri:02", "1");
//	private static final TrackletId TRK_ID2 = new TrackletId("etri:04", "1");
	private @Nullable FOption<BinaryAssociation> associate(MatchingSession session, TrackletFeatureMatrix candidate) {
		if ( candidate != null && candidate.getFeatureCount() > 0 ) {
			Match match = calcTopKDistance(session.m_featureMatrix, candidate, m_topPercent);
			
			long leftTs = session.m_featureMatrix.getTrackFeatureEvent(match.m_leftIndex).getTimestamp();
			long rightTs = candidate.getTrackFeatureEvent(match.m_rightIndex).getTimestamp();
			String id = candidate.getTrackletId().toString();
			
			BinaryAssociation assoc;
			if ( session.m_trkId.compareTo(candidate.getTrackletId()) <= 0 ) {
				assoc = new BinaryAssociation(id, session.m_trkId, candidate.getTrackletId(),
												match.m_score, leftTs, rightTs, candidate.getStartTimestamp());
			}
			else {
				assoc = new BinaryAssociation(id, candidate.getTrackletId(), session.m_trkId,
												match.m_score, rightTs, leftTs, candidate.getStartTimestamp());
			}

			// NOTE: 두 tracklet의 거리를 확인할 때 사용함
//			boolean breakCond = assoc.containsTracklet(TRK_ID1) && assoc.containsTracklet(TRK_ID2);
//			boolean breakCond = session.m_trkId.equals(TRK_ID);
//			if ( breakCond ) {
//				System.out.println("************* " + assoc);
//			}
			return FOption.of(assoc);
		}
		else {
			return FOption.empty();
		}
	}
	
	private void tearDownSession(TrackletId trkId) {
		MatchingSession session = m_sessions.remove(trkId);
		if ( session == null ) {
			return;
		}
		
		for ( TrackletFeatureMatrix candidate: session.m_assocCandidates.values() ) {
			if ( candidate != null && candidate.removeSharing() == 0 ) {
				m_trackletFeaturesCache.remove(candidate.getTrackletId());
			}
		}

		PriorityQueue<MatchingSession> queue = m_pendingQueues.get(session.getNodeId());
		queue.remove(session);
	}
	
	private Match calcTopKDistance(TrackletFeatureMatrix current, TrackletFeatureMatrix target, double percent) {
		int kth = (int)Math.round((current.getFeatureCount() * target.getFeatureCount()) * percent);
		kth = Math.max(kth, 1);
		MinMaxPriorityQueue<Match> heap = MinMaxPriorityQueue.orderedBy(Ordering.natural().reversed())
																.maximumSize(kth)
																.create();
		
		List<float[]> mat1 = current.getFeatures();
		List<float[]> mat2 = target.getFeatures();
		for ( int i =0; i < mat1.size(); ++i ) {
			for ( int j =0; j < mat2.size(); ++j ) {
				double score = 1-Utils.cosineSimilarityNormalized(mat1.get(i), mat2.get(j));
				heap.add(new Match(i, j, score));
			}
		}
		
		Match match = heap.pollLast();
		if ( LOGGER_DIST.isInfoEnabled() ) {
			String msg = String.format("%s <-> %s : kth=%d/%d, score=%.3f",
										current.getTrackletId(), target.getTrackletId(), kth,
										current.getFeatureCount() * target.getFeatureCount(),
										match.getScore());
			LOGGER_DIST.info(msg);
		}
		return match;
	}
	
	private static FStream<Double> newSequenceRatioStream() {
		return FStream.concat(FStream.of(1.15), FStream.of(1.03), FStream.generate(1d, d->d));
	}
	
	private static class Match implements Comparable<Match> {
		private final int m_leftIndex;
		private final int m_rightIndex;
		private final double m_score;
		
		Match(int leftIndex, int rightIndex, double score) {
			m_leftIndex = leftIndex;
			m_rightIndex = rightIndex;
			m_score = score;
		}
		
		public double getScore() {
			return m_score;
		}

		@Override
		public int compareTo(Match o) {
			return Double.compare(m_score,  o.m_score);
		}

		@Override
		public String toString() {
			return String.format("[%d<->%d] %.3f", m_leftIndex, m_rightIndex, m_score);
		}
	}

	static class MatchingSession {
		/** Matching 대상 tracklet 식별자. */
		private final TrackletId m_trkId;
		/** 대상 tracklet이 카메라 시야에 들어올 때의 zone 식별자. */ 
		private String m_enterZone;
		/** 대상 tracklet이 카메라 시야에 처음으로 들어올 때의 timestamp. */
		private long m_startTs;
		/** 지금까지 수집된 자신의 tracklet에 해당하는 feature들의 집합. */
		private final TrackletFeatureMatrix m_featureMatrix;
		/** Association 상태. */
		private State m_state = State.GATHER_FEATURES;
		/** 본 tracklet과 association 대상인 타 노드 tracklet들의 feature 집합. */
		private Map<TrackletId,TrackletFeatureMatrix> m_assocCandidates = Maps.newHashMap();
		
		static enum State {
			/** Association에 필요한 TrackFeature 이벤트를 수집하는 단계. **/
			GATHER_FEATURES,
			/** 추적 대상 tracklet이 enter-zone이 결정되기를 기다리는 상태. **/
			IDENTIFYING_ENTER_ZONE,
			IDENTIFYING_MATCHING_CANDIDATES,
			READY,
			DISABLED,
		};

		MatchingSession(TrackletId trkId) {
			m_trkId = trkId;
			m_featureMatrix = new TrackletFeatureMatrix(trkId, m_startTs);
		}
		
		public TrackletId getTrackletId() {
			return m_trkId;
		}
		
		public String getNodeId() {
			return m_trkId.getNodeId();
		}
		
		State getState() {
			return m_state;
		}
		
		void setState(State state) {
			m_state = state;
		}
		
		void setFirstTimestamp(long ts) {
			m_startTs = ts;
		}
		
		String getEnterZone() {
			return m_enterZone;
		}
		
		void setEnterZone(String zone) {
			m_enterZone = zone;
		}
		
		int getFeatureCount() {
			return m_featureMatrix.getFeatureCount();
		}
		
		void addTrackFeature(TrackFeature tfeat) {
			m_featureMatrix.addTrackFeature(tfeat);
		}
		
		@Override
		public String toString() {
			return String.format("%s: state=%s, enter=%s, nfeats=%d, candidate=%s",
									m_trkId, m_state, m_enterZone, m_featureMatrix.getFeatureCount(),
									m_assocCandidates.keySet());
		}
	}
	
	private static class AutoPurgedBinaryAssociations extends LinkedHashMap<TrackletId, BinaryAssociation> {
		private static final long serialVersionUID = 1L;
		private static final int CACHE_SIZE = 64;
		
		private AutoPurgedBinaryAssociations() {
			super(CACHE_SIZE+1, .75f, false);
		}

		protected boolean removeEldestEntry(Map.Entry<TrackletId, BinaryAssociation> eldest) {
			return size() > CACHE_SIZE;
		}
	}
}
