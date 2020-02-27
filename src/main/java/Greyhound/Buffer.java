package Greyhound;

import javax.script.ScriptEngineManager;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.List;
import java.util.Set;
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
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Query;
import org.influxdb.InfluxDBException.DatabaseNotFoundException;
import static com.mongodb.client.model.Filters.*;
import java.util.concurrent.TimeUnit;

public class Buffer extends BaseWindowedBolt {
	/**
	 * Each buffer recieves tuples from exactly one topic. It fetches the defined
	 * commit interval from the config DB for its topic and collects tuples as long
	 * as the last commit was less then one commit interval ago. When the last
	 * commit is more than one commit interval ago, Buffer calculates the mean of
	 * the collected tuples, emits each reading downstream and writes all points to
	 * the storage DB.
	 */

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db;
	private InfluxDB influx_db;
	private String experiment_name = new String();
	private String mongo_uri = new String();

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;

		experiment_name = (String) topoConf.get("EXPERIMENT_NAME");
		mongo_uri = (String) topoConf.get("MONGO_CONNECTION_URI");
		config_db = new ConfigDB(mongo_uri, experiment_name);
		String influx_server = (String) config_db.readOne("settings", "experiment_config", eq("name", "influx"))
				.get("server");
		influx_db = InfluxDBFactory.connect(influx_server);
	}

	@Override
	public void execute(TupleWindow inputWindow) {

		List<Tuple> tuples = inputWindow.get();
		if (tuples.size() >= 1) {
			Set<String> combined = ((Document) config_db.readOne("settings", "sensors", 
					eq("name", "storm")).get("readings")).keySet();
			Tuple tu = tuples.get(tuples.size() - 1);
			String reading_name = tu.getStringByField("reading_name");
			if (combined.contains(reading_name)) {
				CalculateCombinedReading(tuples);
			} else {
				WriteToStorage(tu);
				collector.emit(new Values(tu.getStringByField("topic"),
						tu.getDoubleByField("timestamp"),
						tu.getStringByField("host"),reading_name,
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
				this_result = ((Integer)eval).doubleValue();
			}
			String topic = tu.getStringByField("topic");
			Double timestamp = tu.getDoubleByField("timestamp");
			String reading_name = tu.getStringByField("reading_name");
			WriteToStorage(topic, timestamp, "", reading_name, this_result);
			collector.emit(new Values(topic, timestamp, "", reading_name, this_result));
		} catch (Exception e) {
			// log message
		}
	}

	private void WriteToStorage(String topic, Double timestamp, String host, String reading_name, Double value) {

		Builder point = Point.measurement(topic).time(timestamp.longValue(), TimeUnit.MILLISECONDS);
		point.addField(reading_name, value);
		if (!host.equals("")) {
			point.tag("host", host);
		}
		try {
			influx_db.setDatabase(experiment_name);
			influx_db.write(point.build());
		} catch (DatabaseNotFoundException e) {
			influx_db.query(new Query("CREATE DATABASE " + experiment_name));
			influx_db.setDatabase(experiment_name);
			influx_db.write(point.build());
		}
	}

	private void WriteToStorage(Tuple tu) {

		String topic = tu.getStringByField("topic");
		Double value = Double.parseDouble(tu.getStringByField("value"));
		Double timestamp = tu.getDoubleByField("timestamp");
		Builder point = Point.measurement(topic).time(timestamp.longValue(), TimeUnit.MILLISECONDS);
		point.addField(tu.getStringByField("reading_name"), value);
		String host = tu.getStringByField("host");
		if (!host.equals("")) {
			point.tag("host", host);
		}
		if (topic.equals("sysmon")) {
			try {
				influx_db.setDatabase("common");
                        	influx_db.write(point.build());
                	} catch (DatabaseNotFoundException e) {
                        	influx_db.query(new Query("CREATE DATABASE common"));
                        	influx_db.setDatabase("common");
                        	influx_db.write(point.build());
			}
		} else {
			try {
				influx_db.setDatabase(experiment_name);
				influx_db.write(point.build());
			} catch (DatabaseNotFoundException e) {
				influx_db.query(new Query("CREATE DATABASE " + experiment_name));
				influx_db.setDatabase(experiment_name);
				influx_db.write(point.build());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value"));
	}
}
