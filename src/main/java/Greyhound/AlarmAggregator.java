package Greyhound;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.bson.Document;

public class AlarmAggregator extends BaseWindowedBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		String experiment_name = (String) topoConf.get("EXPERIMENT_NAME");
		String mongo_uri = (String) topoConf.get("MONGO_CONNECTION_URI");
		config_db = new ConfigDB(mongo_uri, experiment_name);
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuples = inputWindow.get();
		Tuple tu = tuples.get(tuples.size() - 1);
		String msg = new String();
		Double howBad = -1.;
		List<Document> aggregations = config_db.readMany("settings", "alarm_aggregations");
		// get a list 'aggregated_readings' of all reading_names used in an alarm
		// aggregation
		List<String> aggregated_readings = new ArrayList<String>();
		for (Document aggregation : aggregations) {
			List<String> names = (List<String>) aggregation.get("names");
			for (String name : names) {
				if (!aggregated_readings.contains(name)) {
					aggregated_readings.add(name);
				}
			}
			String operation = aggregation.getString("operation");
			double time_window = aggregation.getDouble("time_window") * 1000;
			if (operation.equals("and")) {
				// check if all alarms exist in the given time window simultaneously
				// if yes, send alarm to LogAlarm bolt
				int count = 0;
				for (String name : names) {
					for (Tuple tuple : tuples) {
						if (tuple.getDoubleByField("timestamp") >= System.currentTimeMillis() - time_window) {
							String this_name = "";
							String host = tuple.getStringByField("host");
							if (!host.equals("")) {
								this_name += host + ",";
							}
							this_name += tuple.getStringByField("reading_name") + ","
									+ tuple.getStringByField("alarm_type");
							if (this_name.equals(name)) {
								count += 1;
								howBad = Math.max(howBad, tuple.getDoubleByField("howBad"));
							}
						}
					}
				}
				if (count == names.size()) {
					msg = "Following alarms are all active: \n";
					for (String name : names) {
						String[] split = name.split(",");
						if (split.length == 3) {
							String host = split[0];
							String reading_name = split[1];
							String type = split[2];
							msg += String.format("%s alarm of %s of %s \n ", type, reading_name, host);
						} else {
							String reading_name = split[0];
							String type = split[1];
							msg += String.format("%s alarm of %s \n", type, reading_name);
						}
					}
					collector.emit(new Values(tu.getDoubleByField("timestamp"), howBad, msg));
				}
			} else if (operation.equals("or")) {
				// check if at least one of the alarms happened in the given time window
				// if yes, ??
				int count = 0;
				for (String name : names) {
					for (Tuple tuple : tuples) {
						String this_name = "";
						String host = tuple.getStringByField("host");
						if (!host.equals("")) {
							this_name += host + ",";
						}
						this_name += tuple.getStringByField("reading_name") + ","
								+ tuple.getStringByField("alarm_type");
						if (this_name.equals(name)) {
							count += 1;
							howBad = Math.max(howBad, tuple.getDoubleByField("howBad"));
						}
					}
				}
				if (count >= 1) {
					msg = "At least one of the following alarms is active: \n";
					for (String name : names) {
						String[] split = name.split(",");
						if (split.length == 3) {
							String host = split[0];
							String reading_name = split[1];
							String type = split[2];
							msg += String.format("%s alarm of %s of %s \n ", type, reading_name, host);
						} else {
							String reading_name = split[0];
							String type = split[1];
							msg += String.format("%s alarm of %s \n", type, reading_name);
						}
					}
					collector.emit(new Values(tu.getDoubleByField("timestamp"), howBad, msg));
				}
			}
		}

		String reading_name = tu.getStringByField("reading_name");
		String alarm_type = tu.getStringByField("alarm_type");
		// send alarms without aggregation to LogAlarm bolt
		if (!aggregated_readings.contains(reading_name + "__" + alarm_type)) {
			howBad = tu.getDoubleByField("howBad");
			String topic = tu.getStringByField("topic");
			boolean hasHost = false;
			String host = tu.getStringByField("host");
			if (!host.equals("")) {
				hasHost = true;
			}

			if (alarm_type.equals("pid")) {
				List<Double> additional_values = (List<Double>) tu.getValueByField("additional_parameters");
				Double pid = additional_values.get(0);
				Double lower_threshold = additional_values.get(1);
				Double upper_threshold = additional_values.get(2);
				msg = String.format("Pid alarm for %s measurement %s%s: %.3f is outside alarm range (%.3f, %.3f)",
						tu.getStringByField("topic"), reading_name, hasHost ? " of " + host : "", pid, lower_threshold,
						upper_threshold);
			}

			else if (alarm_type.equals("simple")) {
				List<Double> additional_values = (List<Double>) tu.getValueByField("additional_parameters");
				Double value = additional_values.get(0);
				Double lower_threshold = additional_values.get(1);
				Double upper_threshold = additional_values.get(2);
				msg = String.format("Simple alarm for %s measurement %s%s: %.3f is outside alarm range (%.3f, %.3f)",
						topic, reading_name, hasHost ? " of " + host : "", value, lower_threshold, upper_threshold);
			}

			else if (alarm_type.equals("timesince")) {
				List<Double> additional_values = (List<Double>) tu.getValueByField("additional_parameters");
				Double value = additional_values.get(0);
				Double lower_threshold = additional_values.get(1);
				Double upper_threshold = additional_values.get(2);
				Double max_duration = additional_values.get(3);
				msg = String.format(
						"TimeSince alarm for %s measurement %s%s: %.3f is outside alarm range (%.3f, %.3f) for more than %.0f seconds",
						topic, reading_name, hasHost ? " of " + host : "", value, lower_threshold, upper_threshold,
						max_duration);
			}
			collector.emit(new Values(tu.getDoubleByField("timestamp"), howBad, msg));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "howBad", "msg"));
	}
}
