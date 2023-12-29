package jarvey.streams.assoc.motion;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

import utils.LoggerNameBuilder;
import utils.func.Either;
import utils.func.Tuple;
import utils.stream.FStream;

import jarvey.streams.assoc.BinaryAssociation;
import jarvey.streams.assoc.BinaryAssociationCollection;
import jarvey.streams.assoc.MCMOTConfig.MotionConfigs;
import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.optor.HoppingWindowManager;
import jarvey.streams.optor.Window;
import jarvey.streams.optor.WindowBasedEventCollector;
import jarvey.streams.optor.Windowed;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MotionBinaryTrackletAssociator
		implements Processor<String, NodeTrack, String, Either<BinaryAssociation, TrackletDeleted>> {
	private static final Logger s_logger = LoggerNameBuilder.from(MotionBinaryTrackletAssociator.class)
															.dropSuffix(1)
															.append("binary")
															.getLogger();
	private static final Logger LOGGER_WINDOW = LoggerNameBuilder.plus(s_logger, "window");
	private static final Logger LOGGER_DIST = LoggerNameBuilder.plus(s_logger, "dist");
	private static final Duration PUNCTUATE_INTERVAL = Duration.ofSeconds(1);
	
	private ProcessorContext<String,Either<BinaryAssociation, TrackletDeleted>> m_context;
	private final OverlapAreaRegistry m_areaRegistry;
	private final HoppingWindowManager m_windowManager;
	private final WindowBasedEventCollector<TaggedTrack> m_aggregation;
	private double m_trackDistanceThreshold;
	private final BinaryAssociationCollection m_binaryCollection;
	private Cancellable m_schedule;
	private long m_lastTs = -1;
	private boolean m_dirty = false;

	public MotionBinaryTrackletAssociator(MotionConfigs motionConfigs,
											BinaryAssociationCollection binaryCollection) {
		m_areaRegistry = motionConfigs.getOverlapAreaRegistry();
		m_windowManager = HoppingWindowManager.ofWindowSize(motionConfigs.getMatchingWindowSize());
		if ( motionConfigs.getMatchingGraceTime() != null ) {
			m_windowManager.graceTime(motionConfigs.getMatchingGraceTime());
		}
		if ( motionConfigs.getMatchingAdvanceTime() != null ) {
			m_windowManager.advanceTime(motionConfigs.getMatchingAdvanceTime());
		}
		m_aggregation = new WindowBasedEventCollector<>(m_windowManager);
		
		m_trackDistanceThreshold = motionConfigs.getMaxTrackDistance();
		m_binaryCollection = binaryCollection;
	}

	@Override
	public void init(ProcessorContext<String,Either<BinaryAssociation, TrackletDeleted>> context) {
		m_context = context;
		m_schedule = context.schedule(PUNCTUATE_INTERVAL, PunctuationType.WALL_CLOCK_TIME,
										this::flushLateWindow);
	}

	@Override
	public void close() {
		m_schedule.cancel();
		
		List<Windowed<List<TaggedTrack>>> windoweds = m_aggregation.close();
		for ( Windowed<List<TaggedTrack>> windowed: windoweds ) {
			processWindowedBucket(windowed);
		}
	}

	@Override
	public void process(Record<String,NodeTrack> record) {
		String areaId = record.key();
		NodeTrack track = record.value();
		m_lastTs = Math.max(m_lastTs, track.getTimestamp());
		
		for ( Windowed<List<TaggedTrack>> windowed: m_aggregation.collect(new TaggedTrack(areaId, track)) ) {
			processWindowedBucket(windowed);
		}
		
		m_dirty = true;
	}
	
	private void processWindowedBucket(Windowed<List<TaggedTrack>> windowed) {
		// bucket 단위에서 association을 실시하면 결과 association들이 시간순으로
		// 정렬되지 않을 수도 있기 때문에, 이들을 시간 순서대로 정렬시킨다.
		// 이때, TrackletDelete 이벤트로 함께 포함시켜서 정렬시킨다.
		FStream.from(groupByArea(windowed))
				.flatMapIterable(kv -> associate(m_areaRegistry.get(kv.key), kv.value))
				.sort(r -> r.timestamp())
				.forEach(m_context::forward);
	}
	
	private List<KeyValue<String,Windowed<List<NodeTrack>>>>
	groupByArea(Windowed<List<TaggedTrack>> wtaggeds) {
		// Expire된 window에 속한 모든 node-track event들을 그것이 속한
		// overlap-area에 따라 grouping한다.
		return FStream.from(wtaggeds.value())
						.groupByKey(TaggedTrack::area, TaggedTrack::track)
						.stream()
						.map((a, bkt) -> KeyValue.pair(a, Windowed.of(wtaggeds.window(), bkt)))
						.toList();
	}
	
	private Tuple<Integer,String> toTracksInNodeMsg(String nodeId, List<NodeTrack> tracks) {
		String trksStr = FStream.from(tracks)
								.map(t -> t.isDeleted() ? t.getTrackId()+"D" : t.getTrackId())
								.distinct()
								.join(',');
		return Tuple.of(Integer.parseInt(nodeId.substring(6)), trksStr);
	}
	
	private List<Record<String,Either<BinaryAssociation, TrackletDeleted>>>
	associate(OverlapArea area, Windowed<List<NodeTrack>> wtracks) {
		Window window = wtracks.window();
		long advanceMillis = m_windowManager.advanceTime().toMillis();
		List<NodeTrack> tracks = wtracks.value();
		
		if ( LOGGER_WINDOW.isInfoEnabled() ) {
			String trksStr = FStream.from(tracks)
									.groupByKey(NodeTrack::getNodeId)
									.stream()
									.map((n,trks) -> toTracksInNodeMsg(n, trks))
									.sort(Tuple::_1)
									.map(t -> String.format("%d[%s]", t._1, t._2))
									.join(", ");
			LOGGER_WINDOW.info("{}: {}", window, trksStr);
		}
		
		// 'delete'된 node-track들은 먼저 따로 분리해내 'TrackletDeleted' 이벤트를 생성하여
		// 결과 association 리스트의 마지막에 추가시킨다.
		// Window 정의시 advanceSize와 windowSize보다 작은 경우, 한 track의 delete 이벤트가
		// 여러 bucket에 포함될 수 있기 때문에, 이를 해결하기 위해
		// 이벤트의 ts과 window의 start ts와의 gap이 advance time보다 같거나 작은 경우에만
		// 활용하도록 한다.
		Map<TrackletId,TrackletDeleted> deleteds
			= FStream.from(tracks)
					.filter(NodeTrack::isDeleted)
					.filter(t -> (t.getTimestamp()-window.beginTime()) < advanceMillis)
					.toMap(NodeTrack::getTrackletId,
							trk -> TrackletDeleted.of(trk.getTrackletId(), trk.getTimestamp()));
		
		List<Record<String,Either<BinaryAssociation, TrackletDeleted>>> outRecords = Lists.newArrayList();
		
		List<TrackSlot> slots = FStream.from(tracks)
										.filterNot(NodeTrack::isDeleted)
										.groupByKey(NodeTrack::getTrackletId)
										.stream()
										.map(TrackSlot::new)
										.toList();
		while ( slots.size() >= 2 ) {
			TrackSlot left = slots.remove(0);
			for ( TrackSlot right: slots ) {
				if ( area.existsOverlap(left.getNodeId(), right.getNodeId()) ) {
					BinaryAssociation assoc = TrackSlot.associate(left, right, m_trackDistanceThreshold);
					if ( assoc != null ) {
						if ( m_binaryCollection.add(assoc) ) {
							if ( s_logger.isInfoEnabled() ) {
								s_logger.info("associated: {}", assoc);
							}
							
							outRecords.add(new Record<>(area.getId(), Either.left(assoc), assoc.getTimestamp()));
						}
					}
				}
			}
		}
		
		// delete 이벤트는 마지막으로 추가한다.
		deleteds.forEach((trkId, deleted) -> {
			outRecords.add(new Record<>(area.getId(), Either.right(deleted), deleted.getTimestamp()));
		});
		
		return outRecords;
	}

	private void flushLateWindow(long ts) {
		if ( !m_dirty && m_lastTs >= 0 ) {
			// Punctuation 인터벌동안 event가 도착하지 않은 경우.
			long expectedMs = m_lastTs + PUNCTUATE_INTERVAL.toMillis();
			List<Windowed<List<TaggedTrack>>> windoweds = m_aggregation.progress(expectedMs);
			for ( Windowed<List<TaggedTrack>> windowed: windoweds ) {
				processWindowedBucket(windowed);
			}
		}
		
		m_dirty = false;
	}
	
	private static final class TaggedTrack implements Timestamped {
		private final String m_areaId;
		private final NodeTrack m_track;
		
		private TaggedTrack(String areaId, NodeTrack track) {
			m_areaId = areaId;
			m_track = track;
		}
		
		public String area() {
			return m_areaId;
		}
		
		public NodeTrack track() {
			return m_track;
		}

		@Override
		public long getTimestamp() {
			return m_track.getTimestamp();
		}
		
		@Override
		public String toString() {
			return m_track.toString();
		}
	}
	
	private static class TrackSlot {
		private final TrackletId m_trackletId;
		private final List<NodeTrack> m_tracks;
		
		TrackSlot(TrackletId trkId, List<NodeTrack> tracks) {
			m_trackletId = trkId;
			m_tracks = tracks;
		}
		
		public String getNodeId() {
			return m_trackletId.getNodeId();
		}
		
		@Override
		public String toString() {
			return String.format("%s: %s", m_trackletId, m_tracks);
		}
		
		// NOTE: 두 tracklet의 거리를 확인할 때 사용함
		private static final TrackletId TRK_ID1 = new TrackletId("etri:01", "1");
		private static final TrackletId TRK_ID2 = new TrackletId("etri:07", "1");
		private static BinaryAssociation associate(TrackSlot left, TrackSlot right, double distThreshold) {
			TrackletId leftKey = left.m_trackletId;
			TrackletId rightKey = right.m_trackletId;
			
			// 두 track 집합 사이의 score를 계산한다.
			//
			Tuple<Double,Tuple<Long,Long>> ret = calcDistance(left.m_tracks, right.m_tracks);
			
			// NOTE: 두 tracklet의 거리를 확인할 때 사용함
			// 사용하지 않을 때는 comment-out 시킨다.
//			boolean breakCond = (leftKey.equals(TRK_ID1) && rightKey.equals(TRK_ID2))
//								|| (leftKey.equals(TRK_ID2) && rightKey.equals(TRK_ID1));
//			boolean breakCond = (leftKey.equals(TRK_ID1) || rightKey.equals(TRK_ID1));
//			if ( breakCond ) {
//				System.out.printf("%s(%d) <-> %s(%d): %.3f%n", leftKey, ret._2._1, rightKey, ret._2._2, ret._1);
//				System.out.print("");
//			}
			if ( LOGGER_DIST.isInfoEnabled() ) {
				double gap = (ret._2._1 - ret._2._2) / 1000f;
				String msg = String.format("%s<->%s: %.1fm (gap=%.1fs)", leftKey, rightKey, ret._1, gap);
				LOGGER_DIST.info(msg);
			}
			
 			if ( ret._1 < distThreshold ) {
				double score = 1 - (ret._1 / distThreshold);
				
				long leftFirstTs = left.m_tracks.get(0).getFirstTimestamp();
				long rightFirstTs = right.m_tracks.get(0).getFirstTimestamp();
				
				return new BinaryAssociation(leftKey, leftFirstTs, ret._2._1,
												rightKey, rightFirstTs, ret._2._2, score);
			}
			else {
				return null;
			}
		}
		
		private static Tuple<Double,Tuple<Long,Long>>
		calcDistance(List<NodeTrack> tracks1, List<NodeTrack> tracks2) {
			List<NodeTrack> longPath, shortPath;
			if ( tracks1.size() >= tracks2.size() ) {
				longPath = tracks1;
				shortPath = tracks2;
			}
			else {
				longPath = tracks2;
				shortPath = tracks1;
			}
			
			return FStream.from(longPath)
							.buffer(shortPath.size(), 1)
							.map(path -> calcSplitDistance(path, shortPath))
							.min(t -> t._1)
							.get();
		}
		
		private static Tuple<Double,Tuple<Long,Long>>
		calcSplitDistance(List<NodeTrack> split1, List<NodeTrack> split2) {
			double total = 0;
			double minDist = Double.MAX_VALUE;
			Tuple<Long,Long> tsPair = null;
			
			for ( Tuple<NodeTrack,NodeTrack> pair: FStream.from(split1).zipWith(FStream.from(split2)) ) {
				double dist = pair._1.getLocation().distance(pair._2.getLocation());
				total += dist;
				if ( dist < minDist ) {
					minDist = dist;
					tsPair = Tuple.of(pair._1.getTimestamp(), pair._2.getTimestamp());
				}
			}
			
			return Tuple.of(total / split1.size(), tsPair);
		}
	}
}
