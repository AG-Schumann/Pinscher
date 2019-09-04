package stormcontrol;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.bolt.JoinBolt;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


public class MainTopology {

	private static final Pattern TOPIC_WILDCARD_PATTERN = Pattern.compile("\\b(?!__)\\w+__\\w+\\b");

	public static void main(String[] args) {

		String bootstrap_servers = "localhost:9092";

		Config config = new Config();
		config.setDebug(false);
		config.setMessageTimeoutSecs(600);
		config.setNumWorkers(1);

		KafkaSpout kafkaSpout = new KafkaSpout<>(getKafkaSpoutConfig(bootstrap_servers));

		TopologyBuilder tp = new TopologyBuilder();
		tp.setSpout("KafkaSpout", kafkaSpout);
		tp.setBolt("ConfigBolt", new ConfigBolt()).shuffleGrouping("KafkaSpout");

		// PID alarm
		tp.setBolt("PropBolt", new ProportionalBolt()).shuffleGrouping("ConfigBolt");
		tp.setBolt("IntBolt",
				new IntegralBolt().withWindow(Count.of(3), Count.of(1)), 20)
				.fieldsGrouping("ConfigBolt", new Fields("topic"));
		tp.setBolt("DiffBolt",
				new DifferentiatorBolt().withWindow(Count.of(3), Count.of(1)), 20)
				.fieldsGrouping("ConfigBolt", new Fields("topic"));

		JoinBolt joinPid = new JoinBolt("ConfigBolt", "key").join("IntBolt", "key", "ConfigBolt")
				.join("DiffBolt", "key", "IntBolt").join("PropBolt", "key", "DiffBolt")
				.select("topic, ConfigBolt:timestamp, ConfigBolt:a, ConfigBolt:b, ConfigBolt:c, ConfigBolt:lower_threshold, ConfigBolt:upper_threshold, IntBolt:integral, PropBolt:proportional, DiffBolt:derivative")
				.withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
		tp.setBolt("JoinPid", joinPid, 20).fieldsGrouping("ConfigBolt", new Fields("key"))
				.fieldsGrouping("IntBolt", new Fields("key"))
				.fieldsGrouping("PropBolt", new Fields("key"))
				.fieldsGrouping("DiffBolt", new Fields("key"));

		tp.setBolt("PidBolt", new PidBolt()).shuffleGrouping("JoinPid");

		// send data to influxDB
		tp.setBolt("InfluxBolt", new InfluxBolt()).shuffleGrouping("KafkaBolt");
        /*
        tp.setBolt("ReadingToStorage", new InfluxBolt()).shuffleGrouping("KafkaSpout");
		tp.setBolt("PropToStorage", new InfluxBolt()).shuffleGrouping("PropBolt");
		tp.setBolt("IntToStorage", new InfluxBolt()).shuffleGrouping("IntBolt");
		tp.setBolt("DiffToStorage", new InfluxBolt()).shuffleGrouping("DiffBolt");
		tp.setBolt("PidToStorage", new InfluxBolt()).shuffleGrouping("PidBolt");
        */

		// Submit topology to production cluster
		try {
			StormSubmitter.submitTopology("MainTopology", config, tp.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
		return KafkaSpoutConfig.builder(bootstrapServers, TOPIC_WILDCARD_PATTERN)
				.setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
				.setRetry(getRetryService())
				.setRecordTranslator(
						(r) -> new Values(r.topic(), (double) r.timestamp(),
								Double.parseDouble(r.value()), "reading"),
						new Fields("topic", "timestamp", "value", "type"))
				.setOffsetCommitPeriodMs(10_000).setFirstPollOffsetStrategy(LATEST)
				.setMaxUncommittedOffsets(20).build();
	}

	private static KafkaSpoutRetryService getRetryService() {
		return new KafkaSpoutRetryExponentialBackoff(
				KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
				KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
				KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
	}
}
