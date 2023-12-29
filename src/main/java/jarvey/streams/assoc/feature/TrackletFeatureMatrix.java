package jarvey.streams.assoc.feature;

import java.util.List;

import com.google.common.collect.Lists;

import jarvey.streams.model.TrackletId;
import jarvey.streams.node.TrackFeature;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
class TrackletFeatureMatrix {
	private final TrackletId m_trkId;
	private final long m_exitTs;
	private int m_shareCount;
	private final List<TrackFeature> m_tfevents;
	private final List<float[]> m_features;

	public TrackletFeatureMatrix(TrackletId trkId, long exitTs) {
		m_trkId = trkId;
		m_exitTs = exitTs;
		m_shareCount = 1;
		m_tfevents = Lists.newArrayList();
		m_features = Lists.newArrayList();
	}
	
	public TrackletId getTrackletId() {
		return m_trkId;
	}
	
	public long getStartTimestamp() {
		return m_tfevents.get(0).getTimestamp();
	}
	
	public long getExitTimestamp() {
		return m_exitTs;
	}
	
	public int getFeatureCount() {
		return m_features.size();
	}
	
	public TrackFeature getTrackFeatureEvent(int idx) {
		return m_tfevents.get(idx);
	}
	
	public List<float[]> getFeatures() {
		return m_features;
	}
	
	public void addTrackFeature(TrackFeature tfeat) {
		m_tfevents.add(tfeat);
		m_features.add(tfeat.getFeature());
	}
	
	public boolean isSharing() {
		return m_shareCount > 0;
	}
	
	public void addSharing() {
		++m_shareCount;
	}
	
	public int removeSharing() {
		return --m_shareCount;
	}
	
	@Override
	public String toString() {
		return String.format("%s: nfeats=%d", m_trkId, m_tfevents.size());
	}
}
