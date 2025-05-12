package jarvey.streams.assoc;

import java.util.Map;

import com.google.common.collect.Maps;

import utils.func.FOption;
import utils.stream.KeyValueFStream;

import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationStatusRegistry {
	private final Map<TrackletId,Status> m_statusMap = Maps.newHashMap();
	
	public void register(TrackletId trkId) {
		m_statusMap.computeIfAbsent(trkId, k -> new Status(k));
	}
	
	public FOption<Status> unregister(TrackletId trkId) {
		return FOption.ofNullable(m_statusMap.remove(trkId));
	}
	
	public FOption<Status> getStatus(TrackletId trkId) {
		return FOption.ofNullable(m_statusMap.get(trkId));
	}
	
	public boolean isAssigned(TrackletId trkId) {
		try {
			return m_statusMap.get(trkId).getAssociation().isPresent();
		}
		catch ( Exception e ) {
			e.printStackTrace();
			return false;
		}
	}
	
	public void setAssigned(TrackletId trkId, Association assoc) {
		m_statusMap.get(trkId).setAssociation(assoc);
	}
	
	@Override
	public String toString() {
		return KeyValueFStream.from(m_statusMap)
								.map((id, status) -> ""+status)
								.join(", ");
	}
	
	public class Status implements Timestamped {
		private final TrackletId m_trackletId;
		private FOption<Association> m_assoc = FOption.empty();
		
		public Status(TrackletId trkId) {
			m_trackletId = trkId;
		}
		
		public TrackletId getTrackletId() {
			return m_trackletId;
		}

		@Override
		public long getTimestamp() {
			return m_assoc.map(Association::getTimestamp).getOrElse(-1L);
		}
		
		public boolean isAssociated() {
			return m_assoc.isPresent();
		}
		
		public FOption<Association> getAssociation() {
			return m_assoc;
		}
		
		public void setAssociation(Association assoc) {
			m_assoc = FOption.ofNullable(assoc);
		}
		
		@Override
		public String toString() {
			String statusStr = m_assoc.map(a -> "A").getOrElse("U");
			return String.format("%s[%s]", m_trackletId, statusStr);
		}
	}
}
