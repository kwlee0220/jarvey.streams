package jarvey.streams.processor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class JdbcTopicExporter<K,V> implements KafkaTopicPartitionProcessor<K,V> {
	private final JdbcProcessor m_jdbc;
	
	protected abstract ProcessResult export(Connection conn, TopicPartition tpart,
											List<ConsumerRecord<K,V>> partition) throws SQLException;
	
	protected JdbcTopicExporter(JdbcProcessor jdbc) {
		m_jdbc = jdbc;
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public ProcessResult process(TopicPartition tpart,
												List<ConsumerRecord<K,V>> partition) throws Exception {
		try ( Connection conn = m_jdbc.connect() ) {
			return export(conn, tpart, partition);
		}
	}
}
