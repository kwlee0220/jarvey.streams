package jarvey.streams.rx;

import utils.jdbc.JdbcProcessor;
import utils.jdbc.JdbcRowSource;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class Test2 {
	public static final void main(String... args) throws Exception {
		JdbcProcessor proc = JdbcProcessor.parseString("postgresql:localhost:5432:dna:urc2004:dna");
		
		JdbcRowSource.selectAsString()
						.from(proc)
						.executeQuery("select id from associated_tracklets")
						.fstream()
						.forEach(System.out::println);
		
//		try ( Connection conn = proc.connect() ) {
//			JdbcRowSource.selectAsString()
//						.from(conn)
//						.executeQuery("select id from associated_tracklets")
//						.fstream()
//						.forEach(System.out::println);
//		}
		
//		try ( Connection conn = proc.connect();
//			  PreparedStatement pstmt = conn.prepareStatement("select id, first_ts from associations where first_ts <= ?") ) {
//			pstmt.setLong(1, 11800);
//			
//			JdbcRowSource.selectAsValueList()
//							.from(pstmt.executeQuery())
//							.fstream()
//							.forEach(System.out::println);
//		}
	}
}