package jarvey.streams.assoc.motion;

import static utils.Utilities.checkState;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.DataUtils;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStream;
import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class OverlapArea {
	private final String m_id;
	private final List<Tuple<String,String>> m_overlaps = Lists.newArrayList();
	private final Map<String,Double> m_distanceThresholds = Maps.newHashMap();
	private final Set<String> m_nodes = Sets.newHashSet();
	
	@SuppressWarnings("unchecked")
	public static OverlapArea parse(Map<String,Object> entries) {
		OverlapArea area = new OverlapArea((String)entries.get("id"));
		
		Object overlaps = entries.get("bindings");
		for ( List<String> pair: (List<List<String>>)overlaps) {
			checkState(pair.size() == 2, () -> String.format("Invalid overlap pair: %s", pair));
			area.addOverlap(pair.get(0), pair.get(1));
		}
		
		FStream.from((List<List<String>>)overlaps)
				.forEach(bd -> area.addOverlap(bd.get(0), bd.get(1)));

		Map<String,Object> distanceMap = (Map<String,Object>)entries.get("distance_threshold");
		if ( distanceMap != null ) {
			KVFStream.from(distanceMap)
					.mapValue(DataUtils::asDouble)
					.forEach(area::addDistanceThreshold);
		}
		
		return area;
	}
	
	public OverlapArea(String id) {
		m_id = id;
	}
	
	public String getId() {
		return m_id;
	}
	
	public void addDistanceThreshold(String node, double threshold) {
		m_distanceThresholds.put(node, threshold);
	}
	
	public boolean addOverlap(String node1, String node2) {
		if ( !existsOverlap(node1, node2) ) {
			m_overlaps.add(pair(node1, node2));
			
			m_nodes.add(node1);
			m_nodes.add(node2);
			
			return true;
		}
		else {
			return false;
		}
	}
	
	public boolean existsOverlap(String node1, String node2) {
		Tuple<String,String> pair = pair(node1, node2);
		return FStream.from(m_overlaps)
						.findFirst(t -> pair.equals(t))
						.isPresent();
	}
	
	public double getDistanceThreshold(String node) {
		return FOption.getOrElse(m_distanceThresholds.get(node), Double.MAX_VALUE);
	}
	
	public boolean containsNode(String nodeId) {
		return m_nodes.contains(nodeId);
	}
	
	public Iterable<String> getOverlaps(String nodeId) {
		return FStream.from(m_overlaps)
						.filter(t -> nodeId.equals(t._1) || nodeId.equals(t._2))
						.flatMap(t -> FStream.of(t._1, t._2))
						.filterNot(nodeId::equals)
						.toSet();
	}
	
	@Override
	public String toString() {
		return FStream.from(m_overlaps)
						.map(t -> String.format("%s-%s", t._1, t._2))
						.join(',');
	}
	
	private static Tuple<String,String> pair(String node1, String node2) {
		return node1.compareTo(node2) <= 0 ? Tuple.of(node1, node2) : Tuple.of(node2, node1);
	}
}
