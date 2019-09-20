package stormcontrol;

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
import java.util.regex.Pattern;

public class MainTopology {

	private static final String[] topics = {"pressure", "voltage", "temperature", "current", "status",
        "power", "level", "sysmon", "other"};

	public static void main(String[] args) {
        
		String bootstrap_servers = "localhost:9092";
		int window_length = 600;
		int max_recurrence = 50;
		Config config = new Config();
		config.setMessageTimeoutSecs(666);
		config.setNumWorkers(1);
        config.setDebug(true);

		TopologyBuilder tp = new TopologyBuilder();
		tp.setSpout("KafkaSpout", new KafkaSpout<>(getKafkaSpoutConfig(bootstrap_servers)));
        
		tp.setBolt("Buffer",
				new Buffer().withWindow(new Duration(window_length, TimeUnit.SECONDS), 
                    new Duration(500, TimeUnit.MILLISECONDS)), 1)
				.fieldsGrouping("KafkaSpout", new Fields("type"));
        

		// PID alarm
  
		tp.setBolt("PidConfig", new PidConfig()).shuffleGrouping("Buffer");
		tp.setBolt("PropBolt", new ProportionalBolt()).shuffleGrouping("PidConfig");
		tp.setBolt("IntBolt",
				new IntegralBolt().withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)),
				5).fieldsGrouping("PidConfig", new Fields("host", "reading_name"));
        tp.setBolt("DiffBolt", new DifferentiatorBolt()
				.withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)), 5)
				.fieldsGrouping("PidConfig", new Fields("host", "reading_name"));
		JoinBolt joinPid = new JoinBolt("PidConfig", "key").join("IntBolt", "key", "PidConfig")
				.join("DiffBolt", "key", "IntBolt").join("PropBolt", "key", "DiffBolt")
				.select("type, timestamp, host, reading_name, a, b, c, levels, recurrence,"
						+ " integral, proportional, derivative")
				.withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
		tp.setBolt("JoinPid", joinPid, 5).fieldsGrouping("PidConfig", new Fields("key"))
				.fieldsGrouping("IntBolt", new Fields("key"))
				.fieldsGrouping("PropBolt", new Fields("key"))
				.fieldsGrouping("DiffBolt", new Fields("key"));

		tp.setBolt("PidBolt", new PidBolt()).shuffleGrouping("JoinPid");

		// Time Since alarm
		tp.setBolt("TimeSinceConfig", new TimeSinceConfig()).shuffleGrouping("KafkaSpout");
		tp.setBolt("TimeSinceBolt", new TimeSinceBolt()
				.withWindow(new Duration(window_length, TimeUnit.SECONDS), Count.of(1)), 10)
				.fieldsGrouping("TimeSinceConfig", new Fields("host", "reading_name"));
		//tp.setBolt("CheckAlarm", new CheckAlarm()
		//		.withWindow(Count.of(max_recurrence)), 5)
		//		.shuffleGrouping("PidBolt").shuffleGrouping("TimeSinceBolt");
        
	//	tp.setBolt("ToInflux", new InfluxBolt())
           //.shuffleGrouping("PropBolt")
           // .shuffleGrouping("IntBolt")
           // .shuffleGrouping("DiffBolt")
           //.shuffleGrouping("PidBolt");
           // .shuffleGrouping("KafkaSpout");
        // Submit topology to production cluster
		try {
			StormSubmitter.submitTopology("MainTopology", config, tp.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
		ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
				(r) -> new Values(r.topic(), (double) r.timestamp(), "", String... decode(r.value())),
				new Fields("type", "timestamp", "host", "reading_name", "value"));
		trans.forTopic("sysmon", (r) -> new Values(r.topic(), (double) r.timestamp(),
                    String... decode(r.value())),
				new Fields("type", "timestamp", "host", "reading_name", "value"));

		return KafkaSpoutConfig.builder(bootstrapServers, topics)
				.setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
				.setRetry(getRetryService()).setRecordTranslator(trans).setOffsetCommitPeriodMs(10_000)
				.setFirstPollOffsetStrategy(LATEST).setMaxUncommittedOffsets(2000).build();
	}

	private static String[] decode(String message) {

		// message: "[<host>],<reading_name>,<value>"
		return message.split(",");

	}

	private static KafkaSpoutRetryService getRetryService() {
		return new KafkaSpoutRetryExponentialBackoff(
				KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
				KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
				KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
	}
}
