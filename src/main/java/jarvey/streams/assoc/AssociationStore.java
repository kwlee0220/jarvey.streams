package jarvey.streams.assoc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.func.Funcs;
import utils.jdbc.JdbcProcessor;
import utils.jdbc.JdbcRowSource;
import utils.stream.FStream;

import jarvey.streams.model.TrackletId;
import jarvey.streams.serialization.json.GsonUtils;

import io.reactivex.rxjava3.core.Observable;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AssociationStore implements KeyValueMapper<String, Association, KeyValue<String, Association>> {
	private static final Logger s_logger
			= LoggerFactory.getLogger(AssociationStore.class.getPackage().getName() + ".store");

	private final JdbcProcessor m_jdbc;

	public AssociationStore(JdbcProcessor jdbc) {
		m_jdbc = jdbc;
	}

	@Override
	public KeyValue<String, Association> apply(String key, Association assoc) {
		Association expanded = addAssociation(assoc);
		return KeyValue.pair(expanded.getId(), expanded);
	}

	public Association getAssociation(String assocId) throws SQLException {
		try (Connection conn = m_jdbc.connect()) {
			return readAssociation(conn, assocId);
		}
	}

	public Association getAssociation(TrackletId trkId) throws SQLException {
		try (Connection conn = m_jdbc.connect()) {
			return readAssociation(conn, trkId);
		}
	}
	
	public Observable<Association> rxGetAssociation(TrackletId trkId) {
		return Observable.fromCallable(() -> getAssociation(trkId));
	}

	public Association addAssociation(Association closedAssoc) {
		AssociationCollection coll = new AssociationCollection("closed-associations-merger");

		try (Connection conn = m_jdbc.connect()) {
			conn.setAutoCommit(false);

			// 삽입할 association과 관련된 tracklet들이 기존에 관련된
			// association들을 찾아 collection에 삽입한다.
			List<Record> records = readAssociations(conn, closedAssoc.getTracklets());

			FStream.from(records)
					.map(Record::getAssociation)
					.distinct(Association::getId)
					.cast(Association.class)
					.forEach(coll::add);

			// 새로 추가할 association도 collection에 추가한다.
			coll.add(closedAssoc);

			// best association을 구한다.
			AssociationCollection bestAssocs = coll.getBestAssociations("tmp");
			Association best = Funcs.getFirstOrNull(bestAssocs);
			if ( bestAssocs.size() != 1 && s_logger.isInfoEnabled() ) {
				String losers = FStream.from(bestAssocs)
										.drop(1)
										.map(a -> String.format("%s(%.3f)", a.getTrackletsString(), a.getScore()))
										.join(',');
				String winner = String.format("%s(%.3f)", best.getTrackletsString(), best.getScore());
				s_logger.info("select the best assoc: {} than {}", winner, losers);
			}

			try {
				if ( records.size() > 0 && s_logger.isInfoEnabled() ) {
					s_logger.info("write a merged association: {}", best);
				}

				writeAssociation(conn, best);
				conn.commit();
			}
			catch ( SQLException e ) {
				conn.rollback();
				throw new RuntimeException(e);
			}

			return best;
		}
		catch ( SQLException e ) {
			throw new RuntimeException(e);
		}
	}

	private Association readAssociation(Connection conn, String assocId) throws SQLException {
		String sqlStrFormat = "select json " + "from associations where id = '%s'";
		String sqlStr = String.format(sqlStrFormat, assocId);
		try ( ResultSet rs = m_jdbc.executeQuery(sqlStr, false) ) {
			if ( rs.next() ) {
				return GsonUtils.parseJson(rs.getString(1), Association.class);
			}
			else {
				return null;
			}
		}
	}

	private Association readAssociation(Connection conn, TrackletId trkId) throws SQLException {
		String sqlStrFormat = "select a.json " + "from associated_tracklets t, associations a "
								+ "where t.association = a.id " + "and t.id = '%s'";
		String sqlStr = String.format(sqlStrFormat, trkId.toString());
		try ( ResultSet rs = m_jdbc.executeQuery(sqlStr, false) ) {
			if ( rs.next() ) {
				return GsonUtils.parseJson(rs.getString(1), Association.class);
			}
			else {
				return null;
			}
		}
	}

	/**
	 * 주어진 tracklet 식별자들이 포함된 association들을 검색하여,
	 * (association, tracklet_id)로 구성된 record 객체들의 리스트를 반환함.
	 * 
	 * @param conn
	 * @param trkIds	검색 대상 tracklet 식별자 리스트.
	 * @return
	 * @throws SQLException
	 */
	private List<Record> readAssociations(Connection conn, Iterable<TrackletId> trkIds) throws SQLException {
		String sqlStrFormat = "select t.id, a.json "
								+ "from associated_tracklets t, associations a "
								+ "where t.association = a.id "
								+ "and t.id in (%s)";
		String inLiteral = FStream.from(trkIds)
								.map(tid -> String.format("'%s'", tid.toString()))
								.join(',');
		String sqlStr = String.format(sqlStrFormat, inLiteral);
		
		return JdbcRowSource.selectAsTuple2()
							.from(conn)
							// (tracklet_id, association_json)
							.executeQuery(sqlStr)
							.fstream()
							// 동일 association 경우에는 json이 동일하므로, json을 기준으로 groupby 수행.
							.groupByKey(t -> (String)t._2, t -> (String)t._1)
							// association별로 소속 tracklet_id들의 리스트 생성됨.
							.stream()
							.mapKey((json, v) -> GsonUtils.parseJson(json, Association.class))
							.flatMap((assoc, ids) -> toRecordStream(assoc, ids))
							.toList();
	}

	private FStream<Record> toRecordStream(Association assoc, List<String> tids) {
		return FStream.from(tids)
					.map(TrackletId::fromString)
					.map(trkId -> new Record(trkId, assoc));
	}

	private void writeAssociation(Connection conn, Association assoc) throws SQLException {
		// Association에 포함된 tracklet들이 이전에 포함된 association 정보를 모두 삭제한다. 
		String deleteAssocSqlFormat = "delete from associations where id in (%s)";
		String inLiteral = FStream.from(assoc.getTracklets())
									.map(tid -> String.format("'%s'", tid.toString()))
									.join(',');
		String deleteSqlStr = String.format(deleteAssocSqlFormat, inLiteral);
		try (Statement stmt = conn.createStatement()) {
			stmt.execute(deleteSqlStr);
		}

		// 새 association 정보를 삽입한다.
		String trkIdsStr = FStream.from(assoc.getTracklets())
									.map(TrackletId::toString)
									.sort()
									.join('-');
		String json = GsonUtils.toJson(assoc);
		String insertAssocSql
			= "insert into associations(id, tracklets_str, json, first_ts, ts) "
				+ "values (?, ?, ?, ?, ?) "
				+ "on conflict(id) do update "
				+ "set tracklets_str = ?, "
					+ "json = ?, "
					+ "first_ts = ?,"
					+ "ts = ?";
		try (PreparedStatement pstmt = conn.prepareStatement(insertAssocSql)) {
			pstmt.setString(1, assoc.getId());
			pstmt.setString(2, trkIdsStr);
			pstmt.setString(3, json);
			pstmt.setLong(4, assoc.getFirstTimestamp());
			pstmt.setLong(5, assoc.getTimestamp());
			pstmt.setString(6, trkIdsStr);
			pstmt.setString(7, json);
			pstmt.setLong(8, assoc.getFirstTimestamp());
			pstmt.setLong(9, assoc.getTimestamp());
			pstmt.execute();
		}

		String insertTrackletSql = "insert into associated_tracklets(id, association) " + "values (?, ?) "
				+ "on conflict(id) do update " + "set association = ?";
		try (PreparedStatement pstmt = conn.prepareStatement(insertTrackletSql)) {
			for ( TrackletId trkId : assoc.getTracklets() ) {
				pstmt.setString(1, trkId.toString());
				pstmt.setString(2, assoc.getId());
				pstmt.setString(3, assoc.getId());
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		}
	}

	public static void createTable(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(SQL_CREATE_TABLE_ASSOCIATIONS);
			stmt.executeUpdate(SQL_CREATE_TABLE_TRACKLETS);
			stmt.executeUpdate(SQL_CREATE_INDEX_TRACKLETS);
		}
	}

	public static void dropTable(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("drop table if exists " + TABLE_ASSOCIATIONS);
			stmt.executeUpdate("drop table if exists " + TABLE_TRACKLETS);
		}
	}

	private static final String TABLE_ASSOCIATIONS = "associations";
	private static final String TABLE_TRACKLETS = "associated_tracklets";

	private static final String SQL_CREATE_TABLE_ASSOCIATIONS = "create table " + TABLE_ASSOCIATIONS + " ("
			+ "id varchar not null, " // 1
			+ "tracklets_str varchar not null, " // 2
			+ "json varchar not null, " // 3
			+ "first_ts bigint not null, " // 4
			+ "ts bigint not null, " // 5
			+ "primary key (id)" + ")";

	private static final String SQL_CREATE_TABLE_TRACKLETS = "create table " + TABLE_TRACKLETS + " ("
			+ "id varchar not null, " // 1
			+ "association varchar not null, " // 2
			+ "primary key (id)" + ")";
	private static final String SQL_CREATE_INDEX_TRACKLETS = "create index " + TABLE_TRACKLETS + "_idx on "
			+ TABLE_TRACKLETS + "(association)";

	public static class Record {
		private final TrackletId m_trkId;
		private final Association m_association;

		Record(TrackletId trkId, Association assoc) {
			m_trkId = trkId;
			m_association = assoc;
		}

		public TrackletId getTrackleId() {
			return m_trkId;
		}

		public Association getAssociation() {
			return m_association;
		}

		@Override
		public String toString() {
			return String.format("%s: %s", m_trkId, "" + m_association);
		}
	}
}
