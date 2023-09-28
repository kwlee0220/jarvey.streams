package jarvey.streams.assoc.feature;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;

import utils.func.Either;
import utils.func.Funcs;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

import jarvey.streams.assoc.BinaryAssociation;
import jarvey.streams.assoc.BinaryAssociationCollection;
import jarvey.streams.assoc.feature.FeatureBinaryTrackletAssociator.MatchingSession.State;
import jarvey.streams.assoc.feature.MCMOTNetwork.IncomingLink;
import jarvey.streams.assoc.feature.MCMOTNetwork.ListeningNode;
import jarvey.streams.model.Range;
import jarvey.streams.model.TrackFeatureSerde;
import jarvey.streams.model.TrackletDeleted;
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
		implements ValueMapper<TrackFeature, Iterable<Either<BinaryAssociation,TrackletDeleted>>> {
	private static final Logger s_logger = LoggerFactory.getLogger(FeatureBinaryTrackletAssociator.class);
	private static final int DECISION_COUNT = 5;

	private final MCMOTNetwork m_network;
	private final KeyedUpdateLogs<TrackFeature> m_featureIndexes;
	private final NodeTrackletUpdateLogs m_trackletIndexes;
	private final double m_topPercent;
	
	private BinaryAssociationCollection m_binaryCollection;
	private Map<TrackletId, Candidate> m_featuresCache = Maps.newHashMap();
	private Map<TrackletId, MatchingSession> m_sessions = Maps.newHashMap();

	public FeatureBinaryTrackletAssociator(MCMOTNetwork network, JdbcProcessor jdbc, Properties consumerProps,
											double topPercent, BinaryAssociationCollection binaryCollection) {
		m_network = network;
		
		Deserializer<TrackFeature> featureDeser = TrackFeatureSerde.s_deerializer;
		m_featureIndexes = new KeyedUpdateLogs<>(jdbc, "track_features_index", consumerProps,
												"track-features", featureDeser);
		m_trackletIndexes = new NodeTrackletUpdateLogs(jdbc, "node_tracks_index",
														consumerProps, "node-tracks");
		m_topPercent = topPercent;
		m_binaryCollection = binaryCollection;
	}
	
	@Override
	public Iterable<Either<BinaryAssociation,TrackletDeleted>> apply(TrackFeature tfeat) {
		TrackletId trkId = tfeat.getTrackletId();

		if ( tfeat.isDeleted() ) {
			List<Either<BinaryAssociation,TrackletDeleted>> ret = Lists.newArrayList();
			MatchingSession session = m_sessions.get(tfeat.getTrackletId());
			if ( session != null ) {
				switch ( session.getState() ) {
					case COLLECTING_CANDIDATES:
					case MORE_FEATURES:
					case READY:
						ret = associate(session);
						break;
					default:
						break;
				}
			}
			tearDownSession(trkId);

			TrackletDeleted deleted = TrackletDeleted.of(trkId, tfeat.getTimestamp());
			ret.add(Either.right(deleted));
			return ret;
		}
		
		MatchingSession session = m_sessions.computeIfAbsent(trkId, MatchingSession::new);
		session.addTrackFeature(tfeat);
		
		switch ( session.getState() ) {
			case DISABLED:
				return Collections.emptyList();
			case IDENTIFYING_ENTER_ZONE:
				// Association을 위한 감시 tracklet의 enter-zone을 확인한다.
				if ( !identifyEnterzone(session) ) {
					return Collections.emptyList();
				}
			case COLLECTING_CANDIDATES:
				if ( session.m_candidates.size() == 0 ) {
					if ( !identifyCandidateTracklets(session) ) {
						return Collections.emptyList();
					}
				}
				if ( !collectCandidateFeatures(session) ) {
					return Collections.emptyList();
				}
			case MORE_FEATURES:
				if ( session.getFeatureCount() < DECISION_COUNT ) {
					return Collections.emptyList();
				}
				session.setState(State.READY);
			case READY:
				List<Either<BinaryAssociation,TrackletDeleted>> ret = associate(session);
				session.setState(State.DISABLED);
				return ret;
		}
		throw new AssertionError();
	}
	
	private List<Either<BinaryAssociation,TrackletDeleted>> associate(MatchingSession session) {
		List<Either<BinaryAssociation,TrackletDeleted>> assocList = Lists.newArrayList();
		List<Candidate> candidates = Funcs.filter(session.m_candidates.values(), c -> c!=null);
		for ( Candidate candidate: candidates ) {
			BinaryAssociation assoc = associate(session, candidate);
			if ( assoc != null ) {
				m_binaryCollection.update(assoc);
				assocList.add(Either.left(assoc));
			}
		}
		
		return assocList;
	}
	
	private boolean identifyEnterzone(MatchingSession session) {
		if ( session.m_enterZone != null ) {
			return true;
		}
		
		TrackletId trkId = session.m_trkId;
		NodeTrackletIndex trackletIndex = m_trackletIndexes.getIndex(trkId);
		if ( trackletIndex != null ) {
			session.m_startTs = trackletIndex.getTimestampRange().min();
			session.m_enterZone = trackletIndex.getEnterZone();
			if ( session.m_enterZone != null ) {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("The watching tracklet' enter-zone is identified: tracklet={}, zone={}",
										trkId, session.m_enterZone);
				}
				session.m_state = State.COLLECTING_CANDIDATES;
				return true;
			}
		}
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("The watching tracklet' enter-zone is not identified: tracklet={}", trkId);
		}
		
		return false;
	}
	
	private boolean identifyCandidateTracklets(MatchingSession session) {
		// Association 후보 tracklet을 찾기 위해 본 tracklet의 enter-zone과
		// tracklet 시작 시각을 이용하여 진입 link들을 찾는다. 
		// 
		ListeningNode listeningNode = m_network.getListeningNode(session.m_trkId.getNodeId());
		List<IncomingLink> incomingLinks = listeningNode.getIncomingLinks(session.m_enterZone);
		if ( incomingLinks.isEmpty() ) {
			// 예측되는 이전 카메라 정보가 없는 경우는 추적을 포기한다.
			session.setState(State.DISABLED);
			return false;
		}
		
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("found the candidate cameras: {}:{}->, {}",
							session.m_trkId, session.m_enterZone, incomingLinks);
		}

		for ( IncomingLink link: incomingLinks ) {
			// 이전 노드에서의 candidate tracklet의 예상 exit 시간 구간을 계산하여
			// 해당 구간에 exit한 tracklet들을 후보들의 NodeTrackletIndex 정보는 읽어온다.
			Range<Duration> transRange = link.getTransitionTimeRange();
			long minTs = session.m_startTs - transRange.max().toMillis();
			long maxTs = session.m_startTs - transRange.min().toMillis();
			String whereClause
				= String.format("node = '%s' and exit_zone='%s' and last_ts between %d and %d",
								link.getExitNode(), link.getExitZone(), minTs, maxTs);
			// FIXME: 나중에 삭제함
			if ( session.m_trkId.equals(new TrackletId("etri:07", "2")) ) {
				System.out.print("");
			}
			for ( NodeTrackletIndex index: m_trackletIndexes.findIndexes(whereClause) ) {
				TrackletId trkId = index.getTrackletId();
				Candidate candidate = m_featuresCache.get(trkId);
				if ( candidate != null ) {
					candidate.incrementShareCount();
				}
				session.m_candidates.put(trkId, candidate);
			}
		}
		if ( session.m_candidates.size() > 0 ) {
			session.setState(State.COLLECTING_CANDIDATES);
			return true;
		}
		else {
			session.setState(State.DISABLED);
			return false;
		}
	}
	
	private boolean collectCandidateFeatures(MatchingSession session) {
		// collecting unloaded candidates
		List<String> loadingTrkIds = FStream.from(session.m_candidates)
												.filterValue(c -> c == null)
												.map((t, c) -> t.toString())
												.toList();
		
		boolean fullyLoaded = true;
		for ( KeyedUpdateIndex index: m_featureIndexes.readIndexes(loadingTrkIds).values() ) {
			if ( !index.isClosed() ) {
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("skip for the unfinished tracklet's features: id={}", index.getKey());
				}
				fullyLoaded = false;
				continue;
			}

			TrackletId trkId = TrackletId.fromString(index.getKey());
			Candidate candidate = new Candidate(trkId);
			m_featureIndexes.streamUpdatesOfIndex(index)
				 			.map(kv -> kv.value)
				 			.filterNot(TrackFeature::isDeleted)
				 			.forEach(candidate::addTrackFeature);
			 m_featuresCache.put(trkId, candidate);
			 session.m_candidates.put(trkId, candidate);
			 
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("candidate tracklet's features are ready: target={}, nfeatures={}",
								index.getKey(), candidate.getFeatureCount());
			}
		}
		if ( fullyLoaded ) {
			if ( session.getFeatureCount() >= DECISION_COUNT ) {
				session.setState(State.READY);
			}
			else {
				session.setState(State.MORE_FEATURES);
			}
		}
		
		return fullyLoaded;
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
															.groupByKey(k -> m_featuresCache.get(k) != null);
			cases.switcher()
				 .ifCase(true).consume(this::shareCandidate)	// 이미 cache된 경우는 share-count만 증가.
				 .ifCase(false).consume(this::downloadCandidateFeatures);
		}
		else {
			String sqlString = String.format("select * from %s where %s",
											m_trackletIndexes.getIndexTableName(), whereClause);
			s_logger.warn("fails to find candidate tracks: sql={}", sqlString);
		}
		
		return candidateTrkIds;
	}
	
	private void downloadCandidateFeatures(List<TrackletId> trkIds) {
		List<TrackletId> missingTrkIds = Funcs.asNonNull(trkIds, Collections.emptyList());
		if ( missingTrkIds.size() > 0 ) {
			List<String> missingKeys = Funcs.map(missingTrkIds, TrackletId::toString);
			for ( KeyedUpdateIndex index: m_featureIndexes.readIndexes(missingKeys).values() ) {
				if ( !index.isClosed() ) {
					if ( s_logger.isDebugEnabled() ) {
						s_logger.debug("skip for the unfinished tracklet's features: id={}", index.getKey());
					}
					continue;
				}

				TrackletId trkId = TrackletId.fromString(index.getKey());
				Candidate candidate = new Candidate(trkId);
				m_featureIndexes.streamUpdatesOfIndex(index)
					 			.map(kv -> kv.value)
					 			.filterNot(TrackFeature::isDeleted)
					 			.forEach(candidate::addTrackFeature);
				 m_featuresCache.put(trkId, candidate);
				if ( s_logger.isInfoEnabled() ) {
					s_logger.info("candidate tracklet's features are ready: target={}, nfeatures={}",
									index.getKey(), candidate.getFeatureCount());
				}
			}
		}
	}
	
	private void shareCandidate(List<TrackletId> trkIds) {
		trkIds.forEach(trkId -> {
			Candidate candidate = m_featuresCache.get(trkId);
			candidate.incrementShareCount();
		});
	}
	
	private @Nullable BinaryAssociation associate(MatchingSession session, Candidate candidate) {
		if ( candidate != null && candidate.m_trackFeatures.size() > 0 ) {
			Match match = calcTopKDistance(session, candidate, m_topPercent);
			
			long leftTs = session.m_trackFeatures.get(match.m_leftIndex).getTimestamp();
			long rightTs = candidate.m_trackFeatures.get(match.m_rightIndex).getTimestamp();
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

			// FIXME: 나중에 삭제함
			if ( assoc.containsTracklet(new TrackletId("etri:07", "1"))
				&& assoc.containsTracklet(new TrackletId("etri:07", "2")) ) {
				System.out.println("************* " + assoc);
			}
			return assoc;
		}
		else {
			return null;
		}
	}
	
	private void tearDownSession(TrackletId trkId) {
		MatchingSession session = m_sessions.remove(trkId);
		if ( session == null ) {
			return;
		}
		
		for ( Candidate candidate: session.m_candidates.values() ) {
			if ( candidate != null && --candidate.m_shareCount == 0 ) {
				m_featuresCache.remove(candidate.m_trkId);
			}
		}
	}
	
	private Match calcTopKDistance(MatchingSession session, Candidate candidate, double percent) {
		int kth = (int)Math.round((session.getFeatureCount() * candidate.getFeatureCount()) * percent);
		kth = Math.max(kth, 1);
		MinMaxPriorityQueue<Match> heap = MinMaxPriorityQueue.orderedBy(Ordering.natural().reversed())
																.maximumSize(kth)
																.create();
		
		List<float[]> mat1 = session.m_featureList;
		List<float[]> mat2 = candidate.m_featureList;
		for ( int i =0; i < mat1.size(); ++i ) {
			for ( int j =0; j < mat2.size(); ++j ) {
				double score = 1-Utils.cosineSimilarityNormalized(mat1.get(i), mat2.get(j));
				heap.add(new Match(i, j, score));
			}
		}
		
		Match match = heap.pollLast();
		if ( s_logger.isInfoEnabled() ) {
			String msg = String.format("%s <-> %s : count=%d, kth=%d, score=%.3f",
										session.m_trkId, candidate.getTrackletId(),
										session.getFeatureCount() * candidate.getFeatureCount(), kth,
										match.getScore());
			s_logger.info(msg);
		}
		return match;
	}

	private static class Candidate {
		private final TrackletId m_trkId;
		private int m_shareCount;
		private final List<TrackFeature> m_trackFeatures;
		private final List<float[]> m_featureList;

		Candidate(TrackletId trkId) {
			m_trkId = trkId;
			m_shareCount = 1;
			m_trackFeatures = Lists.newArrayList();
			m_featureList = Lists.newArrayList();
		}
		
		TrackletId getTrackletId() {
			return m_trkId;
		}
		
		long getStartTimestamp() {
			return m_trackFeatures.get(0).getTimestamp();
		}
		
		int getFeatureCount() {
			return m_featureList.size();
		}
		
		void addTrackFeature(TrackFeature tfeat) {
			m_trackFeatures.add(tfeat);
			m_featureList.add(tfeat.getFeature());
		}
		
		void incrementShareCount() {
			++m_shareCount;
		}
		
		@Override
		public String toString() {
			return String.format("%s: nfeats=%d", m_trkId, m_trackFeatures.size());
		}
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
		
		public int getLeftIndex() {
			return m_leftIndex;
		}
		
		public int getRightIndex() {
			return m_rightIndex;
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
		private final TrackletId m_trkId;
		private String m_enterZone;
		private long m_startTs;
		private final List<TrackFeature> m_trackFeatures;
		private final List<float[]> m_featureList;
		private State m_state = State.IDENTIFYING_ENTER_ZONE;
		private Map<TrackletId,Candidate> m_candidates = Maps.newHashMap();
		
		static enum State {
			IDENTIFYING_ENTER_ZONE,
			COLLECTING_CANDIDATES,
			MORE_FEATURES,
			READY,
			DISABLED,
		};

		MatchingSession(TrackletId trkId) {
			m_trkId = trkId;
			m_trackFeatures = Lists.newArrayList();
			m_featureList = Lists.newArrayList();
		}
		
		State getState() {
			return m_state;
		}
		
		int getFeatureCount() {
			return m_featureList.size();
		}
		
		void setState(State state) {
			m_state = state;
		}
		
		void addTrackFeature(TrackFeature tfeat) {
			m_trackFeatures.add(tfeat);
			m_featureList.add(tfeat.getFeature());
		}
		
		@Override
		public String toString() {
			return String.format("%s: state=%s, enter=%s, nfeats=%d, candidate=%s",
									m_trkId, m_state, m_enterZone, m_trackFeatures.size(),
									m_candidates.keySet());
		}
	}
}
