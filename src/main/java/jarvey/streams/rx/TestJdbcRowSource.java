package jarvey.streams.rx;

import java.sql.Connection;
import java.sql.PreparedStatement;

import utils.jdbc.JdbcProcessor;
import utils.jdbc.JdbcRowSource;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TestJdbcRowSource {
	public static final void main(String... args) throws Exception {
		JdbcProcessor proc = JdbcProcessor.parseString("postgresql:localhost:5432:dna:urc2004:dna");
		
//		JdbcRowSource.select(rs -> rs.getString(1))
//						.from(proc)
//						.executeQuery("select id from associated_tracklets")
//						.observe()
//						.subscribe(System.out::println);
		
//		try ( Connection conn = proc.connect() ) {
//			JdbcRowSource.select(rs -> rs.getString(1))
//							.from(conn)
//							.executeQuery("select id from associated_tracklets")
//							.observe()
//							.subscribe(System.out::println);
//		}
		
		try ( Connection conn = proc.connect() ) {
			PreparedStatement pstmt = conn.prepareStatement("select id, first_ts from associations "
															+ "where first_ts <= ?");
			pstmt.setLong(1, 1698984323000L);
			
			JdbcRowSource.selectAsTuple2()
							.from(pstmt.executeQuery())
							.observe()
							.subscribe(System.out::println);
		}
	}
}