package jarvey.streams;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Maps;

import utils.func.Funcs;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class KafkaAdmins {
	private final Properties m_adminProps;
	
	public KafkaAdmins(String bootstrapServers) {
		m_adminProps = new Properties();
		m_adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	}
	
	public void createTopic(String topicName, int partCount,
									int replicaFactor) throws InterruptedException, ExecutionException {
		try ( Admin admin = Admin.create(m_adminProps) ) {
			NewTopic newTopic = new NewTopic(topicName, partCount, (short)replicaFactor);
			CreateTopicsResult result = admin.createTopics(Arrays.asList(newTopic));
			KafkaFuture<Void> future = result.values().get(topicName);
			future.get();
		}
	}
	
	public boolean existsTopic(String topicName) throws InterruptedException, ExecutionException {
		return listTopics(topicName, false).contains(topicName);
	}
	
	public Set<String> listTopics(String topicName, boolean listInternals)
		throws InterruptedException, ExecutionException {
		try ( AdminClient client = AdminClient.create(m_adminProps) ) {
			ListTopicsOptions opts = new ListTopicsOptions();
			opts.listInternal(listInternals);
			
			ListTopicsResult topics = client.listTopics(opts);
			return topics.names().get();
		}
	}
	
	public TopicDescription describeTopic(String topicName)
		throws InterruptedException, ExecutionException {
		try ( AdminClient client = AdminClient.create(m_adminProps) ) {
			DescribeTopicsResult result = client.describeTopics(Collections.singleton(topicName));
			return result.all().get().get(topicName);
		}
	}
	
	// FIXME: 현재 작업 중인데 레코드를 delete 하는 방법을 잘몰라서 작성하다가 중단됨.
	public void purgeRecords(String topicName) throws InterruptedException, ExecutionException {
		TopicDescription topicDesc = describeTopic(topicName);
		
		try ( AdminClient client = AdminClient.create(m_adminProps) ) {
			Map<TopicPartition,RecordsToDelete> recordsToDelete = Maps.newHashMap();
			Map<TopicPartition,OffsetSpec> specs = Maps.newHashMap();
			OffsetSpec.forTimestamp(-1);
		}
	}
	
	public Map<String,TopicDescription> describeTopics(Collection<String> topicNames)
		throws InterruptedException, ExecutionException {
		try ( AdminClient client = AdminClient.create(m_adminProps) ) {
			DescribeTopicsResult result = client.describeTopics(topicNames);
			return result.all().get();
		}
	}
	
	public void deleteTopic(String topicName)
		throws InterruptedException, ExecutionException {
		try ( AdminClient client = AdminClient.create(m_adminProps) ) {
			DeleteTopicsResult result = client.deleteTopics(Collections.singleton(topicName));
			result.all().get();
		}
	}
	
	public List<String> listConsumerGroups() throws InterruptedException, ExecutionException {
		try ( AdminClient client = AdminClient.create(m_adminProps) ) {
			return Funcs.map(client.listConsumerGroups().all().get(), ConsumerGroupListing::groupId);
		}
	}
	
	public void deleteConsumerGroups(Collection<String> grpIds)
		throws InterruptedException, ExecutionException {
		try ( AdminClient client = AdminClient.create(m_adminProps) ) {
			DeleteConsumerGroupsResult result = client.deleteConsumerGroups(grpIds);
			result.all().get();
		}
	}
}
