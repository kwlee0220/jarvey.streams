package jarvey.streams.assoc.tool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.UsageHelp;

import jarvey.streams.KafkaAdmins;
import jarvey.streams.model.TrackFeatureSerde;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.processor.AbstractKafkaTopicProcessorDriver;
import jarvey.streams.processor.KafkaConsumerRecordProcessor;
import jarvey.streams.processor.KafkaTopicPartitionProcessor;
import jarvey.streams.processor.KafkaTopicPartitionProcessor.ProcessResult;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="time-node-events",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="Repartition the node-tracks based on their overlap-areas.")
public class TimeNodeEventsMain extends AbstractKafkaTopicProcessorDriver<String,byte[]> {
	private static final Logger s_logger = LoggerFactory.getLogger(TimeNodeEventsMain.class);

	private static final String DEFAULT_GROUP_ID = "build-global-tracks";
	private static final String INPUT_TOPIC = "track-features";
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;

	@Override
	protected String getDefaultGroupId() {
		return DEFAULT_GROUP_ID;
	}

	@Override
	protected Collection<String> getInputTopics() {
		return Arrays.asList(INPUT_TOPIC);
	}

	@Override
	protected KafkaTopicPartitionProcessor<String,byte[]> allocateProcessor(TopicPartition tpart)
		throws IOException {
		Accumulator accum = new Accumulator();
		return KafkaTopicPartitionProcessor.wrap(accum);
	}

	@Override
	public void run() throws Exception {
		KafkaAdmins admin = new KafkaAdmins(m_kafkaOptions.getBootstrapServersOrDefault());
		try {
			if ( !admin.existsTopic(INPUT_TOPIC) ) {
				s_logger.warn("input topic('{}') is not present. create one", INPUT_TOPIC);
				admin.createTopic(INPUT_TOPIC, 1, 1);
			}
		}
		catch ( Exception e ) {
			s_logger.warn("fails to get topic description", e);
		}
		
		super.run();
	}
	
	public static final void main(String... args) throws Exception {
		TimeNodeEventsMain cmd = new TimeNodeEventsMain();
		
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.run();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
	
	private static class Accumulator implements KafkaConsumerRecordProcessor<String,byte[]> {
		private final Deserializer<TrackFeature> m_deserializer;
		private long m_minElapsed = Long.MAX_VALUE;
		private long m_maxElapsed = Long.MIN_VALUE;
		private int m_count = 0;
		private long m_sum = 0;
		
		public Accumulator() {
			Serde<TrackFeature> serde = TrackFeatureSerde.getInstance();
			m_deserializer = serde.deserializer();
		}

		@Override
		public void close() throws Exception {
			double avg = m_sum / (double)m_count;
			System.out.printf("avg=%.0f, min=%d, max=%d%n", avg, m_minElapsed, m_maxElapsed);
		}

		@Override
		public ProcessResult process(TopicPartition tpart, ConsumerRecord<String, byte[]> record) {
			TrackFeature tfeat = m_deserializer.deserialize(record.topic(), record.value());
			
			long now = System.currentTimeMillis();
			long elapsedMillis = now - tfeat.getTimestamp();
			if ( elapsedMillis > 10000 ) {
				System.out.printf("%s: %d%n", tfeat, elapsedMillis);
			}
			m_minElapsed = Math.min(m_minElapsed, elapsedMillis);
			m_maxElapsed = Math.max(m_maxElapsed, elapsedMillis);
			m_sum += elapsedMillis;
			++m_count;
			
			return ProcessResult.of(tpart, record.offset() + 1, false);
		}

		@Override
		public ProcessResult timeElapsed(TopicPartition tpart, long expectedTs) {
			double avg = m_sum / (double)m_count;
			System.out.printf("avg=%.0f, min=%d, max=%d%n", avg, m_minElapsed, m_maxElapsed);
			
			return ProcessResult.NULL;
		}

		@Override
		public long extractTimestamp(ConsumerRecord<String, byte[]> record) {
			return m_deserializer.deserialize(record.topic(), record.value()).getTimestamp();
		}
	}
}
