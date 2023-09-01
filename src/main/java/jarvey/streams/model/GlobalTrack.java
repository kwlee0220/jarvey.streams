package jarvey.streams.model;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Point;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import utils.func.Funcs;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;


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
		UNASSOCIATED("U"),
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
		// supporting track들에서 평균 위치를 계산한다.
		List<Point> pts = Funcs.map(supports, LocalTrack::getLocation);
		Point avgPt = GeoUtils.average(pts);
		long ts = Funcs.max(supports, LocalTrack::getTimestamp).getTimestamp();
		
		return new GlobalTrack(assoc.getId(), State.ASSOCIATED, overlapArea, avgPt, supports,
								assoc.getFirstTimestamp(), ts);
	}
	
	public static final GlobalTrack from(AssociationClosure.DAO assoc, List<LocalTrack> supports,
											@Nullable String overlapArea) {
		// supporting track들에서 평균 위치를 계산한다.
		List<Point> pts = Funcs.map(supports, LocalTrack::getLocation);
		Point avgPt = GeoUtils.average(pts);
		long ts = Funcs.max(supports, LocalTrack::getTimestamp).getTimestamp();
		
		return new GlobalTrack(assoc.getId(), State.ASSOCIATED, overlapArea, avgPt, supports,
								assoc.getFirstTimestamp(), ts);
	}
	
	public static final GlobalTrack from(Association assoc, LocalTrack ltrack, @Nullable String overlapArea) {
		return new GlobalTrack(assoc.getId(), State.ASSOCIATED, null, ltrack.getLocation(),
								Collections.singletonList(ltrack), ltrack.getFirstTimestamp(),
								ltrack.getTimestamp());
	}
	
	public static final GlobalTrack from(Iterable<LocalTrack> ltracks, String overlapArea) {
		LocalTrack leader = Funcs.min(ltracks, LocalTrack::getFirstTimestamp);
		
		List<LocalTrack> supports = Funcs.removeIf(ltracks, LocalTrack::isDeleted);
		if ( supports.isEmpty() ) {
			return GlobalTrack.deleted(leader, overlapArea);
		}
		
		// supporting track들에서 평균 위치를 계산한다.
		List<Point> pts = Funcs.map(supports, LocalTrack::getLocation);
		Point avgPt = GeoUtils.average(pts);
		
		long ts = Funcs.max(supports, LocalTrack::getTimestamp).getTimestamp();
		
		return new GlobalTrack(leader.getKey(), State.ASSOCIATED, overlapArea, avgPt, supports,
								leader.getFirstTimestamp(), ts);
	}
	
	public static final GlobalTrack from(LocalTrack ltrack, @Nullable String overlapArea) {
		return new GlobalTrack(ltrack.getKey(), State.ASSOCIATED, overlapArea, ltrack.getLocation(),
								null, ltrack.getFirstTimestamp(), ltrack.getTimestamp());
	}

	public static final GlobalTrack deleted(AssociationClosure assoc, LocalTrack ltrack,
											@Nullable String overlapArea) {
		return new GlobalTrack(assoc.getId(), State.DELETED, overlapArea, null,
								Collections.singletonList(ltrack),
								ltrack.getFirstTimestamp(), ltrack.getTimestamp());
	}
	public static final GlobalTrack deleted(AssociationClosure.DAO assoc, LocalTrack ltrack,
											@Nullable String overlapArea) {
		return new GlobalTrack(assoc.getId(), State.DELETED, overlapArea, null,
								Collections.singletonList(ltrack),
								ltrack.getFirstTimestamp(), ltrack.getTimestamp());
	}
	public static final GlobalTrack deleted(LocalTrack ltrack, @Nullable String overlapArea) {
		return new GlobalTrack(ltrack.getKey(), State.DELETED, overlapArea, null, null,
								ltrack.getFirstTimestamp(), ltrack.getTimestamp());
	}
	
	public static final GlobalTrack merged(Association assoc, LocalTrack ltrack) {
		return new GlobalTrack(ltrack.getKey(), State.MERGED, null, null, null,
								ltrack.getFirstTimestamp(), ltrack.getTimestamp());
	}
	
	private GlobalTrack(String id, State state, String overlapArea, Point loc, List<LocalTrack> ltracks,
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
		if ( m_location == null ) {	// deleted track
			return String.format("%s:deleted#%d", getKey(), getTimestamp());
		}
		
		String locStr = m_location != null ? GeoUtils.toString(m_location, 1) : "none";
		if ( m_supports != null ) {
			String supportStr = FStream.from(m_supports)
										.map(LocalTrack::getTrackletId)
										.sort()
										.map(TrackletId::toString)
										.join('-');
			return String.format("%s%s#%d {%s} (%d)",
									getKey(), area, m_ts, supportStr, m_firstTs);
		}
		else {
			return String.format("%s%s#%d {%s} (%d)", getKey(), area, m_ts, locStr, m_firstTs);
		}
	}
}