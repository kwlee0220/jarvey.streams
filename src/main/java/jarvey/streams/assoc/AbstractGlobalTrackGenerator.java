package jarvey.streams.assoc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Point;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.func.Funcs;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

import jarvey.streams.assoc.GlobalTrack.State;
import jarvey.streams.assoc.motion.OverlapArea;
import jarvey.streams.assoc.motion.OverlapAreaRegistry;
import jarvey.streams.model.TrackletId;
import jarvey.streams.node.NodeTrack;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractGlobalTrackGenerator {
	private static final long INTERVAL = 100;

	private final @Nullable OverlapAreaRegistry m_areaRegistry;
	
	private final List<LocalTrack> m_trackBuffer = Lists.newArrayList();
	private long m_firstTs = -1;
	
	private final Map<String,Set<TrackletId>> m_runningTrackletsMap = Maps.newHashMap();
	
	protected abstract Association findAssociation(LocalTrack ltrack);
	
	protected AbstractGlobalTrackGenerator(@Nullable OverlapAreaRegistry areaRegistry) {
		m_areaRegistry = areaRegistry;
	}
	
	public List<GlobalTrack> generate(NodeTrack track) {
		// NodeTrack 이벤트가 overlap area에 포함되었는지 여부에 따라
		// global track을 생성하는 방법이 달라짐.
		if ( m_areaRegistry == null ) {
			return toNonOverlapAreaGlobalTracks(track);
		}
		else {
			return m_areaRegistry.findByNodeId(track.getNodeId())
									.map(area -> toOverlapAreaGlobalTracks(area, track))
									.getOrElse(() -> toNonOverlapAreaGlobalTracks(track));
		}
	}

	public List<GlobalTrack> closeGenerator() throws Exception {
		if ( m_trackBuffer.size() > 0 ) {
			List<GlobalTrack> gtracks = processTrackBatch(m_trackBuffer);
			m_trackBuffer.clear();
			
			return gtracks;
		}
		else {
			return Collections.emptyList();
		}
	}

	public List<GlobalTrack> handleTimeElapsed(long expectedTs) {
		if ( m_trackBuffer.size() > 0 && (expectedTs - m_firstTs) >= INTERVAL ) {
			List<GlobalTrack> gtracks = processTrackBatch(m_trackBuffer);
			m_trackBuffer.clear();
			
			return gtracks;
		}
		else {
			return Collections.emptyList();
		}
	}
	
	private List<GlobalTrack> toOverlapAreaGlobalTracks(OverlapArea area, NodeTrack track) {
		// 카메라로부터 일정 거리 이내의 track 정보만 활용한다.
		// 키 값도 node-id에서 overlap-area id로 변경시킨다.
		if ( !withAreaDistance(area, track) ) {
			return Collections.emptyList();
		}
		
		LocalTrack ltrack = LocalTrack.from(track);
		
		long ts = ltrack.getTimestamp();
		if ( m_firstTs < 0 ) {
			m_firstTs = ltrack.getTimestamp();
		}

		// 일정기간 동안 track을 모아서 한번에 처리하도록 한다. (예: 100ms)
		// 지정된 기간이 넘으면 모인 track에 대해 combine을 수행한다.
		// 만일 기간을 경과하지 않으면 바로 반환한다.
		if ( (ts - m_firstTs) < INTERVAL ) {
			m_trackBuffer.add(ltrack);
			return Collections.emptyList();
		}
		
		List<GlobalTrack> gtracks = processTrackBatch(m_trackBuffer);
		m_trackBuffer.clear();
		m_trackBuffer.add(ltrack);
		m_firstTs = ltrack.getTimestamp();
		
		return gtracks;
	}
	
	private List<GlobalTrack> processTrackBatch(List<LocalTrack> ltracks) {
		// 'delete' track을 먼저 따로 뽑는다.
		KeyedGroups<Association, LocalTrack> deleteds = FStream.from(ltracks)
																.filter(lt -> lt.isDeleted())
																.tagKey(lt -> findAssociation(lt))
																.groupByKey();

		// 버퍼에 수집된 local track들을 association에 따라 분류한다.
		// 만일 overlap area에 포함되지 않는 track의 경우에는 별도로 지정된
		// null로 분류한다.
		KeyedGroups<Association, LocalTrack> groups
				= FStream.from(m_trackBuffer)
						.filter(lt -> !lt.isDeleted())
						.tagKey(lt -> findAssociation(lt))
						.groupByKey();
		
		// association이 존재하는 경우는 동일 assoication끼리 묶어 평균 값을 사용한다.
		List<GlobalTrack> gtracks = groups.fstream()
											.filter(kv -> kv.key() != null)
											.map(kv -> average(kv.key(), kv.value()))
											.toList();
		
		// Association이 없는 track들은 각 trackletId별로 하나의 global track을 생성한다.
		List<LocalTrack> unassociateds = groups.remove(null)
												.getOrElse(Collections.emptyList());
		FStream.from(unassociateds)
				.tagKey(lt -> lt.getTrackletId())
				.groupByKey()
				.fstream()
				.map(kv -> average(kv.value()))
				.forEach(gtracks::add);
		
		// delete event의 경우는 association 별로 소속 tracklet이 모두 delete되는
		// 경우에만 delete global track을 추가하되, 이때도 association id를 사용한다.
		deleteds.fstream()
				.flatMap(kv -> FStream.from(handleTrackletDeleted((Association)kv.key(), kv.value())))
				.forEach(gtracks::add);
		
		// 생성된 global track들을 timestamp를 기준으로 정렬시킨다.
		gtracks = FStream.from(gtracks)
						.sort(GlobalTrack::getTimestamp)
						.toList();
		
		return gtracks;
	}
	
	private List<GlobalTrack> toNonOverlapAreaGlobalTracks(NodeTrack track) {
		LocalTrack ltrack = LocalTrack.from(track);
		
		// track과 관련된 association을 찾는다.
		Association assoc = findAssociation(ltrack);

		GlobalTrack gtrack =  GlobalTrack.from(ltrack, assoc);
		return Collections.singletonList(gtrack);	
	}
	
	private boolean withAreaDistance(OverlapArea area, NodeTrack track) {
		if ( track.isDeleted() ) {
			return true;
		}
		
		double threshold = area.getDistanceThreshold(track.getNodeId());
		return track.getDistance() <= threshold;
	}
	
	private String getOverlapAreaId(LocalTrack ltrack) {
		return m_areaRegistry.findByNodeId(ltrack.getNodeId())
								.map(OverlapArea::getId)
								.getOrNull();
	}
	
	private GlobalTrack average(Association assoc, List<LocalTrack> ltracks) {
		String areaId = m_areaRegistry.findByNodeId(ltracks.get(0).getNodeId())
										.map(OverlapArea::getId)
										.getOrNull();
		return GlobalTrack.from(assoc, ltracks);
	}
	
	private GlobalTrack average(List<LocalTrack> ltracks) {
		LocalTrack repr = Funcs.max(ltracks, LocalTrack::getTimestamp).get();
		String id = repr.getKey();
		Point avgLoc = GeoUtils.average(Funcs.map(ltracks, LocalTrack::getLocation));
		
		String areaId = m_areaRegistry.findByNodeId(ltracks.get(0).getNodeId())
										.map(OverlapArea::getId)
										.getOrNull();
		return new GlobalTrack(id, State.ISOLATED, avgLoc, null,
								repr.getFirstTimestamp(), repr.getTimestamp());
	}
	
	private List<GlobalTrack> handleTrackletDeleted(Association assoc, List<LocalTrack> deleteds) {
		if ( assoc == null ) {
			return Funcs.map(deleteds, lt -> GlobalTrack.from(lt, assoc));
		}
		
		// Association에 참여하는 tracklet들의 delete 여부를 association별로 관리한다.
		// tracklet의 종료하는 경우 'm_runningTrackletsMap'에서 제거하고
		// Association에 속하는 모든 tracklet들이 모두 종료될 때 association id를 사용한
		// Deleted 이벤트를 발생시킨다.
		Set<TrackletId> participants = m_runningTrackletsMap.computeIfAbsent(assoc.getId(),
																		k -> Sets.newHashSet(assoc.getTracklets()));
		deleteds.forEach(lt -> participants.remove(lt.getTrackletId()));
		if ( participants.isEmpty() ) {
			// Association에 참여하는 모든 tracklet들이 종료된 경우.
			m_runningTrackletsMap.remove(assoc.getId());
			
			// Association의 id와 소속 tracklet들 중에서 가장 마지막으로 종료된 tracklet의
			// timestamp를 이용하여 deleted 이벤트를 생성한다.
			LocalTrack last = Funcs.max(deleteds, LocalTrack::getTimestamp).get();
			GlobalTrack gtrack = new GlobalTrack(assoc.getId(), State.DELETED,
												null, null, assoc.getFirstTimestamp(), last.getTimestamp());
			return Collections.singletonList(gtrack);
		}
		else {
			return Collections.emptyList();
		}
	}
}
