package Pinscher;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.StormSubmitter;

import static com.mongodb.client.model.Filters.eq;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.LATEST;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Topology {
	/*
	 * Class that initializes the Storm topology for a given MongoDB URI and
	 * experiment name and submits it to the production cluster.
	 */
	private static ConfigDB config_db;

	public static void main(String[] args) {
		/*
		 * :args[0] mongo_uri: Connection URI to MongoDB with write access 
		 * :args[1] experiment_name: name of the experiment. Will also become name of the topology
		 */
		String mongo_uri = new String();
		String experiment_name = new String();
		try {
			mongo_uri = args[0];
			experiment_name = args[1];
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("Topology requires two arguments: MONGO_URI, EXPERIMENT_NAME");
		}

		int window_length = 600; // maximum length for dt_diff, dt_int, max_duration and time window of
									// alarm aggregations
		int max_recurrence = 50; // maximum value for recurrence (simple and pid alarm)
		Map<String, Object> config = new HashMap<String, Object>();
		config.put("MONGO_CONNECTION_URI", mongo_uri);
		config.put("EXPERIMENT_NAME", experiment_name);
		config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, (int) 1.1 * window_length);
		config_db = new ConfigDB(mongo_uri, experiment_name);
		String bootstrap_servers = (String) config_db
				.readOne("settings", "experiment_config", eq("name", "kafka")).get("bootstrap_servers");

		TopologyBuilder tp = new TopologyBuilder();
		tp.setSpout("KafkaSpout",
				new KafkaSpout<>(getKafkaSpoutConfig(bootstrap_servers, experiment_name)));
		tp.setBolt("ReadingAggregator",
				new ReadingAggregator().withWindow(new Duration(60, TimeUnit.SECONDS), Count.of(1)))
				.shuffleGrouping("KafkaSpout");
		tp.setBolt("WriteToStorage", new WriteToStorage(), 5).shuffleGrouping("ReadingAggregator");

		// PID alarm
		tp.setBolt("PidConfig", new PidConfig()).shuffleGrouping("ReadingAggregator");
		tp.setBolt("PropBolt", new ProportionalBolt()).shuffleGrouping("PidConfig");
		tp.setBolt("IntBolt", new IntegralBolt()
				.withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)))
				.fieldsGrouping("PropBolt", new Fields("host", "reading_name"));
		tp.setBolt("DiffBolt", new DifferentiatorBolt()
				.withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)))
				.fieldsGrouping("IntBolt", new Fields("host", "reading_name"));
		tp.setBolt("CheckPid", new CheckPid().withWindow(Count.of(max_recurrence), Count.of(1)))
				.fieldsGrouping("DiffBolt", new Fields("reading_name"));

		// Time Since alarm
		tp.setBolt("TimeSinceConfig", new TimeSinceConfig()).shuffleGrouping("ReadingAggregator");
		tp.setBolt("TimeSinceBolt",
				new TimeSinceBolt().withWindow(new Duration(window_length, TimeUnit.SECONDS),
						Count.of(1)))
				.fieldsGrouping("TimeSinceConfig", new Fields("host", "reading_name"));
		tp.setBolt("CheckTimeSince", new CheckTimeSince(), 5).shuffleGrouping("TimeSinceBolt");

		// Simple alarm
		tp.setBolt("SimpleConfig", new SimpleConfig(), 1).shuffleGrouping("ReadingAggregator");
		tp.setBolt("CheckSimple", new CheckSimple().withWindow(Count.of(max_recurrence), Count.of(1)))
				.fieldsGrouping("SimpleConfig", new Fields("reading_name"));

		tp.setBolt("AlarmAggregator",
				new AlarmAggregator().withWindow(new Duration(window_length, TimeUnit.SECONDS),
						Count.of(1)))
				.shuffleGrouping("CheckPid").shuffleGrouping("CheckTimeSince")
				.shuffleGrouping("CheckSimple");
		tp.setBolt("LogAlarm", new LogAlarm()).shuffleGrouping("AlarmAggregator");
		// Submit topology to production cluster
		try {
			StormSubmitter.submitTopology(experiment_name, config, tp.createTopology());
		} catch (Exception e) {
			System.out.println("Could not sumbmit topology: " + e);
		}
	}

	private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers,
			String experiment_name) {
		/*
		 * Returns the configuration of the Kafka spout.
		 * 
		 * :param bootsrapServers: comma-separated list of host and port pairs that are
		 * the addresses of the brokers of the Kafka cluster 
		 * :param experiment_name: name of the experiment; the Spout will subscribe to all Kafka topics
		 * starting with this name.
		 */
		ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
				(r) -> new Values(r.topic().split("_")[1], (double) r.timestamp(), "",
						decode(r.value(), 2)[0], decode(r.value(), 2)[1]),
				new Fields("topic", "timestamp", "host", "reading_name", "value"));
		trans.forTopic(experiment_name + "_sysmon",
				(r) -> new Values(r.topic().split("_")[1], (double) r.timestamp(), decode(r.value(), 3)[0],
						decode(r.value(), 3)[1], decode(r.value(), 3)[2]),
				new Fields("topic", "timestamp", "host", "reading_name", "value"));
		return KafkaSpoutConfig
				.builder(bootstrapServers, Pattern.compile("^" + experiment_name + "_.*$"))
				.setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup").setRetry(getRetryService())
				.setRecordTranslator(trans).setOffsetCommitPeriodMs(10_000)
				.setFirstPollOffsetStrategy(LATEST).setMaxUncommittedOffsets(200000).build();
	}

	private static String[] decode(String message, int limit) {
		/*
		 * Splits comma separated messages into its parts 
		 * :param message: "[<host>],<reading_name>,<value>"
		 */
		return message.split(",", limit);

	}
	
	private static KafkaSpoutRetryService getRetryService() {
		return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
				KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
				KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
	}

}
