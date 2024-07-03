package jarvey.streams.assoc;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import utils.func.Funcs;

import jarvey.streams.assoc.motion.OverlapAreaRegistry;
import jarvey.streams.node.NodeTrack;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RunningGlobalTrackGenerator extends AbstractGlobalTrackGenerator
											implements KeyValueMapper<String, NodeTrack,
																	Iterable<KeyValue<String,GlobalTrack>>> {
	private final AssociationCollection m_associations;
	private final AssociationCollection m_finalAssociations;
	
	public RunningGlobalTrackGenerator(OverlapAreaRegistry areaRegistry, AssociationCollection associations,
										AssociationCollection finalAssociations) {
		super(areaRegistry);
		
		m_associations = associations;
		m_finalAssociations = finalAssociations;
	}

	@Override
	public Iterable<KeyValue<String,GlobalTrack>> apply(String areaId, NodeTrack track) {
		return Funcs.map(generate(track), g -> KeyValue.pair(g.getKey(), g));
	}

	@Override
	protected Association findAssociation(LocalTrack ltrack) {
		// 현재까지의 association들 중에서 superior들만 추린다.
		AssociationCollection bestAssocs = m_associations.getBestAssociations("global-track-associations");
		
		// 이미 close된 association들을 추가하여 best association 리스트를 생성한다.
		m_finalAssociations.forEach(bestAssocs::add);
		
		bestAssocs = bestAssocs.getBestAssociations("global-track-associations");
		Association best = Funcs.findFirst(bestAssocs, a -> a.containsTracklet(ltrack.getTrackletId()))
								.getOrNull();
		
		return best;
	}
}
