package Greyhound;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.StormSubmitter;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.LATEST;

import java.util.concurrent.TimeUnit;

public class MainTopology {

	private static final String[] topics = { "pressure", "voltage", "temperature", "current", "status", "power",
			"level", "sysmon", "other" };

	public static void main(String[] args) {
		String bootstrap_servers = "localhost:9092";
		int window_length = 600;
		int max_recurrence = 50;
		Config config = new Config();
		config.setMessageTimeoutSecs(666);
		config.setNumWorkers(1);
		config.setDebug(true);

		TopologyBuilder tp = new TopologyBuilder();
		// KafkaSpout emits tuples to streams named after their topics
		tp.setSpout("KafkaSpout", new KafkaSpout<>(getKafkaSpoutConfig(bootstrap_servers)));
		// split the stream and ensure that all tuples of similar type go to exactly one
		// buffer
		tp.setBolt("StreamSplitter", new StreamSplitter()).shuffleGrouping("KafkaSpout");
		tp.setBolt("Buffer", new Buffer().withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)),
				topics.length).directGrouping("StreamSplitter", "direct_stream");

		// PID alarm
		tp.setBolt("PidConfig", new PidConfig(), 5).shuffleGrouping("Buffer");

		tp.setBolt("PropBolt", new ProportionalBolt()).shuffleGrouping("PidConfig");

		tp.setBolt("IntBolt", new IntegralBolt().withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)),
				5).fieldsGrouping("PidConfig", new Fields("host", "reading_name"));

		tp.setBolt("DiffBolt",
				new DifferentiatorBolt().withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)), 5)
				.fieldsGrouping("PidConfig", new Fields("host", "reading_name"));

		JoinBolt joinPid = new JoinBolt("PidConfig", "key").join("IntBolt", "key", "PidConfig")
				.join("DiffBolt", "key", "IntBolt").join("PropBolt", "key", "DiffBolt")
				.select("topic, timestamp, host, reading_name, a, b, c, levels, recurrence,"
						+ " integral, proportional, derivative")
				.withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
		tp.setBolt("JoinPid", joinPid, 5).fieldsGrouping("PidConfig", new Fields("key"))
				.fieldsGrouping("IntBolt", new Fields("key")).fieldsGrouping("PropBolt", new Fields("key"))
				.fieldsGrouping("DiffBolt", new Fields("key"));

		tp.setBolt("PidBolt", new PidBolt()).shuffleGrouping("JoinPid");
		tp.setBolt("CheckPid", new CheckPid().withWindow(Count.of(max_recurrence), Count.of(1)), 5)
				.fieldsGrouping("PidBolt", new Fields("reading_name"));

		// Time Since alarm
		tp.setBolt("TimeSinceConfig", new TimeSinceConfig(), 5).shuffleGrouping("Buffer");
		tp.setBolt("TimeSinceBolt",
				new TimeSinceBolt().withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)), 10)
				.fieldsGrouping("TimeSinceConfig", new Fields("host", "reading_name"));
		tp.setBolt("CheckTimeSince", new CheckTimeSince(), 5).shuffleGrouping("TimeSinceBolt");
		
		// Simple alarm
		tp.setBolt("SimpleConfig", new SimpleConfig(), 5).shuffleGrouping("Buffer");
		tp.setBolt("CheckSimple", new CheckSimple().withWindow(Count.of(max_recurrence), Count.of(1)), 5)
				.fieldsGrouping("SimpleConfig", new Fields("reading_name"));

		tp.setBolt("AlarmAggregator",
				new AlarmAggregator().withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)))
				.shuffleGrouping("CheckPid")
				.shuffleGrouping("CheckTimeSince")
				.shuffleGrouping("CheckSimple");
		tp.setBolt("LogAlarm", new LogAlarm()).shuffleGrouping("AlarmAggregator");
		
		// Submit topology to production cluster
		try {
			StormSubmitter.submitTopology("MainTopology", config, tp.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
		ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
				(r) -> new Values(r.topic(), (double) r.timestamp(), "", decode(r.value())[0], decode(r.value())[1]),
				new Fields("topic", "timestamp", "host", "reading_name", "value"));
		trans.forTopic(
				"sysmon", (r) -> new Values(r.topic(), (double) r.timestamp(), decode(r.value())[0],
						decode(r.value())[1], decode(r.value())[2]),
				new Fields("topic", "timestamp", "host", "reading_name", "value"));

		return KafkaSpoutConfig.builder(bootstrapServers, topics)
				.setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup").setRetry(getRetryService())
				.setRecordTranslator(trans).setOffsetCommitPeriodMs(10_000).setFirstPollOffsetStrategy(LATEST)
				.setMaxUncommittedOffsets(2000).build();
	}

	private static String[] decode(String message) {

		// message: "[<host>],<reading_name>,<value>"
		return message.split(",");

	}

	private static KafkaSpoutRetryService getRetryService() {
		return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
				KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
				KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
	}
}
