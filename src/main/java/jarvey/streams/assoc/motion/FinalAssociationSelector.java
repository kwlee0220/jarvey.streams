package jarvey.streams.assoc.motion;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.LoggerSettable;
import utils.func.Funcs;
import utils.stream.FStream;

import jarvey.streams.assoc.Association;
import jarvey.streams.assoc.AssociationCollection;
import jarvey.streams.assoc.BinaryAssociation;
import jarvey.streams.assoc.BinaryAssociationCollection;
import jarvey.streams.model.TrackletDeleted;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FinalAssociationSelector implements ValueMapper<TrackletDeleted, Iterable<Association>>, LoggerSettable {
	private static final Logger s_logger
		= LoggerFactory.getLogger(FinalAssociationSelector.class.getPackage().getName() + ".final");

	private final BinaryAssociationCollection m_binaryCollection;
	private final AssociationCollection m_associations;
	private final Set<TrackletId> m_closedTracklets;
	private Logger m_logger;
	
	public static final TrackletId TRK_ID = MotionAssociationStreamBuilder.TRK_ID;
	
	public FinalAssociationSelector(BinaryAssociationCollection binaryCollection,
									AssociationCollection associations, Set<TrackletId> closedTracklets) {
		m_binaryCollection = binaryCollection;
		m_associations = associations;
		m_closedTracklets = closedTracklets;
		
		setLogger(s_logger);
	}

	@Override
	public Iterable<Association> apply(TrackletDeleted deleted) {
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("tracklet deleted: {}", deleted.getTrackletId());
		}
		
		// FIXME: 디버깅 후 삭제
		if ( deleted.getTrackletId().equals(TRK_ID) ) {
			System.out.print("");
		}
		
		List<Association> selectedClosedAssocList = handleTrackDeleted(deleted);
		
		// 최종적으로 선택된 association closure에 포함된 tracklet들과 연관된
		// 모든 binary association들을 제거한다.
		selectedClosedAssocList.forEach(this::purgeClosedBinaryAssociation);
		
		if ( selectedClosedAssocList.size() > 0 ) {
			return FStream.from(selectedClosedAssocList)
						.sort(Association::getTimestamp)
						.toList();
		}
		else {
			return Collections.emptyList();
		}
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}

	private List<Association> handleTrackDeleted(TrackletDeleted deleted) {
		// 주어진 tracklet의 delete로 인해 해당 tracklet과 연관된 association들 중에서
		// fully closed된 association만 뽑는다.
		List<Association> fullyCloseds
				= Funcs.filter(m_associations, cl -> m_closedTracklets.containsAll(cl.getTracklets()));
		// 뽑은 association들 중 일부는 서로 conflict한 것들이 있을 수 있기 때문에
		// 이들 중에서 점수를 기준으로 conflict association들을 제거한 best association 집합을 구한다.
		fullyCloseds = AssociationCollection.selectBestAssociations(fullyCloseds);
		if ( s_logger.isDebugEnabled() && fullyCloseds.size() > 0 ) { 
			for ( Association cl: fullyCloseds ) {
				s_logger.debug("fully-closed: {}", cl);
			}
		}
		
		// 만일 유지 중인 association들 중에서 fully-closed 상태가 아니지만,
		// best association보다 superior한 것이 존재하면 해당 best association의 graduation을 대기시킴.
		List<Association> graduated = Lists.newArrayList();
		while ( fullyCloseds.size() > 0 ) {
			Association closed = Funcs.removeFirst(fullyCloseds);
			
			// close된 closure보다 더 superior한 closure가 있는지 확인하여
			// 없는 경우에만 관련 closure 삭제를 수행한다.
			Association superior = m_associations.findSuperiorFirst(closed);
			if ( superior != null ) {
				// 'closed'보다 superior한 closure가 존재하는 경우
				// 선택하지 않는다.
				if ( s_logger.isDebugEnabled() ) {
					s_logger.debug("fails to select due to a superior: this={} superior={}", closed, superior);
				}
			}
			else {
				graduate(closed);
				graduated.add(closed);
			}
		}
		
		// 검색된 최종 final association에 등록된 모든 tracklet에 대한
		// 종료 정보를 (더 이상 필요없기 때문에) 제거한다.
		FStream.from(graduated)
				.flatMap(assoc -> FStream.from(assoc.getTracklets()));
		
//		TrackletId TRK_ID = new TrackletId("etri:01", "19");
//		if ( m_closedTracklets.contains(TRK_ID) ) {
//			System.out.println("\t" + m_associations.find(TRK_ID));
//		}

		return graduated;
	}
		
	private void graduate(Association closure) {
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("final associations: {}", closure);
		}
		m_associations.remove(closure.getTracklets());
		m_associations.resolveConflict(closure);
	}
	
	private void purgeClosedBinaryAssociation(Association assoc) {
		// 주어진 tracklet이 포함된 모든 binary association을 제거한다.
		List<BinaryAssociation> purgeds = m_binaryCollection.removeAll(assoc.getTracklets());
		if ( s_logger.isDebugEnabled() && purgeds.size() > 0 ) {
			purgeds.forEach(ba -> s_logger.debug("delete binary-association: {}", ba));
		}
	}
}
