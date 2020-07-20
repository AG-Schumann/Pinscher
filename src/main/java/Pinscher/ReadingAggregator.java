package Pinscher;

import javax.script.ScriptEngineManager;
import javax.script.ScriptEngine;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.bson.Document;
import static com.mongodb.client.model.Filters.*;

public class ReadingAggregator extends BaseWindowedBolt {
	/**
	 * The ReadingAggregator receives tuples from the KafkaSpout.
	 * 
	 * Each buffer receives tuples from exactly one topic. It fetches the defined
	 * commit interval from the config DB for its topic and collects tuples as long
	 * as the last commit was less then one commit interval ago. When the last
	 * commit is more than one commit interval ago, Buffer calculates the mean of
	 * the collected tuples, emits each reading downstream and writes all points to
	 * the storage DB.
	 */

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db;
	private String experiment_name = new String();
	private String mongo_uri = new String();
	private Document combined = new Document();
	private long last_update = 0;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
			OutputCollector collector) {

		this.collector = collector;

		experiment_name = (String) topoConf.get("EXPERIMENT_NAME");
		mongo_uri = (String) topoConf.get("MONGO_CONNECTION_URI");
		config_db = new ConfigDB(mongo_uri, experiment_name);
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		/*
		 * Checks each tuple if it belongs to a combined reading (i.e. the sensor is
		 * storm) If yes it is calculated, else the tuple is emitted unchanged
		 */

		List<Tuple> tuples = inputWindow.get();
		if (tuples.size() >= 1) {
			// refresh config file every second
			long current_time = System.currentTimeMillis();
			if (current_time - last_update >= 1000) {
				combined = config_db.readOne("settings", "sensors", eq("name", "storm"));
				last_update = current_time;
			}

			Set<String> combined_readings = new HashSet<String>();
			try {
				combined_readings = ((Document) combined.get("readings")).keySet();
			} catch (Exception e) {
				String msg = "Can\'t access combinded readings: " + e;
				config_db.log(msg, 10);
			}
			Tuple tu = tuples.get(tuples.size() - 1);
			String reading_name = tu.getStringByField("reading_name");
			if (combined_readings.contains(reading_name)) {
				CalculateCombinedReading(tuples);
			} else {
				collector.emit(new Values(tu.getStringByField("topic"), tu.getDoubleByField("timestamp"),
						tu.getStringByField("host"), reading_name,
						Double.parseDouble(tu.getStringByField("value"))));
			}
		}
	}

	private void CalculateCombinedReading(List<Tuple> tuples) {

		Tuple tu = tuples.get(tuples.size() - 1);
		String operation = tu.getStringByField("value");
		// Create a Map: '<reading_name>[,<host>]' : '<latest_value>'
		Map<String, String> latest_readings = new HashMap<String, String>();
		for (int i = tuples.size() - 1; i >= 0; --i) {
			Tuple this_tuple = tuples.get(i);
			String host = this_tuple.getStringByField("host");
			String key = this_tuple.getStringByField("reading_name");
			if (!host.equals("")) {
				key += "," + host;
			}
			if (!latest_readings.containsKey(key)) {
				latest_readings.put(key, this_tuple.getStringByField("value"));
			}
		}
		// replace names of single_readings with the latest value
		for (String key : latest_readings.keySet()) {
			operation = operation.replace(key, latest_readings.get(key));
		}
		Double this_result = 0.0;
		try {
			ScriptEngineManager mgr = new ScriptEngineManager();
			ScriptEngine engine = mgr.getEngineByName("JavaScript");
			Object eval = engine.eval(operation);
			if (eval instanceof Double) {
				this_result = (Double) eval;
			} else if (eval instanceof Integer) {
				this_result = ((Integer) eval).doubleValue();
			}
			String topic = tu.getStringByField("topic");
			Double timestamp = tu.getDoubleByField("timestamp");
			String reading_name = tu.getStringByField("reading_name");
			collector.emit(new Values(topic, timestamp, "", reading_name, this_result));
		} catch (Exception e) {
			String msg = "Can\'t calculate cominded readings " + tu.getStringByField("reading_name")
					+ " : " + e;
			config_db.log(msg, 20);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value"));
	}
}
