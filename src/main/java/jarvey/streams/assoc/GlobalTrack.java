package jarvey.streams.assoc;

import static utils.Utilities.checkArgument;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Point;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import utils.func.Funcs;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;

import jarvey.streams.model.ObjectTrack;
import jarvey.streams.model.TrackletId;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GlobalTrack implements ObjectTrack {
	@SerializedName("id") private String m_id;
	@SerializedName("state") private State m_state;
	@Nullable @SerializedName("overlap_area") String m_overlapArea;
	@Nullable @SerializedName("location") Point m_location;				// delete인 경우는 null
	@Nullable @SerializedName("supports") List<LocalTrack> m_supports;	// un-associated인 경우는 null
	@SerializedName("first_ts") private long m_firstTs;
	@SerializedName("ts") private long m_ts;
	
	public enum State {
		ASSOCIATED("A"),
		MERGED("M"),
		ISOLATED("I"),
		DELETED("D");
		
		private String m_code;
		
		private State(String code) {
			m_code = code;
		}
		
		public String getCode() {
			return m_code;
		}
		
		public State fromCode(String code) {
			return State.valueOf(code);
		}
	};
	
	public static final GlobalTrack from(Association assoc, List<LocalTrack> supports,
											@Nullable String overlapArea) {
		checkArgument(assoc != null, "assoc is null");
		
		// supporting track들에서 평균 위치를 계산한다.
		List<Point> pts = Funcs.map(supports, LocalTrack::getLocation);
		Point avgPt = GeoUtils.average(pts);
		long ts = Funcs.max(supports, LocalTrack::getTimestamp).getTimestamp();
		
		return new GlobalTrack(assoc.getId(), State.ASSOCIATED, overlapArea, avgPt, supports,
								assoc.getFirstTimestamp(), ts);
	}
	
	public static final GlobalTrack from(LocalTrack ltrack, @Nullable Association assoc, 
											@Nullable String overlapArea) {
		if ( ltrack.isDeleted() ) {
			return new GlobalTrack(ltrack.getKey(), State.DELETED, overlapArea, null, null,
									ltrack.getFirstTimestamp(), ltrack.getTimestamp());
		}
		else if ( assoc != null ) {
			return new GlobalTrack(assoc.getId(), State.ASSOCIATED, overlapArea, ltrack.getLocation(),
									Collections.singletonList(ltrack), ltrack.getFirstTimestamp(),
									ltrack.getTimestamp());
		}
		else {
			return new GlobalTrack(ltrack.getKey(), State.ISOLATED, overlapArea, ltrack.getLocation(),
									null, ltrack.getFirstTimestamp(), ltrack.getTimestamp());
		}
	}
	
	public static final GlobalTrack from(Iterable<LocalTrack> ltracks, String overlapArea) {
		if ( Funcs.exists(ltracks, LocalTrack::isDeleted) ) {
			throw new IllegalArgumentException("ltracks contains 'deleted' track.");
		}
		
		LocalTrack leader = Funcs.min(ltracks, LocalTrack::getFirstTimestamp);
		
		// supporting track들에서 평균 위치를 계산한다.
		List<Point> pts = Funcs.map(ltracks, LocalTrack::getLocation);
		Point avgPt = GeoUtils.average(pts);
		
		long ts = Funcs.max(ltracks, LocalTrack::getTimestamp).getTimestamp();
		
		return new GlobalTrack(leader.getKey(), State.ISOLATED, overlapArea, avgPt, null,
								leader.getFirstTimestamp(), ts);
	}
	
	public GlobalTrack(String id, State state, String overlapArea, Point loc, List<LocalTrack> ltracks,
						long firstTs, long ts) {
		m_id = id;
		m_state = state;
		m_overlapArea = overlapArea;
		m_location = loc;
		m_supports = ltracks;
		m_firstTs = firstTs;
		m_ts = ts;
	}

	@Override
	public String getKey() {
		return m_id;
	}
	
	public State getState() {
		return m_state;
	}
	
	public boolean isDeleted() {
		return m_state == State.DELETED;
	}
	
	public Point getLocation() {
		return m_location;
	}
	
	public String getOverlapArea() {
		return m_overlapArea;
	}
	
	public List<LocalTrack> getLocalTracks() {
		return m_supports;
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public boolean isSameTrack(GlobalTrack other) {
		return Objects.equal(m_id, other.m_id);
	}
	
	@Override
	public String toString() {
		String area = m_overlapArea != null ? String.format("@%s", m_overlapArea) : "";
		
		String locStr = m_location != null ? GeoUtils.toString(m_location, 1) : "";
		String supportsStr = "";
		if ( m_supports != null ) {
			TrackletId leaderId = TrackletId.fromString(m_id);
			List<TrackletId> trkIds = FStream.from(m_supports)
												.map(LocalTrack::getTrackletId)
												.sort()
												.toList();
			supportsStr = String.format(" {%s}", Association.toString(leaderId, trkIds));
		}
		
		return String.format("%s%s %s %s#%d%s (%d)",
							getKey(), area, m_state.toString().substring(0,1),
							locStr, m_ts, supportsStr, m_firstTs);
	}
}