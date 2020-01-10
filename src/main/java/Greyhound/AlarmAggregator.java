package Greyhound;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
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
		config_db = new ConfigDB();
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuples = inputWindow.get();
		List<Tuple> new_tuples = inputWindow.getNew();

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
							String this_name = tuple.getStringByField("reading_name") + "__"
									+ tuple.getStringByField("alarm_type");
							if (this_name == name) {
								count += 1;
							}
						}
					}
				}
				if (count == names.size()) {
					collector.emit(new Values());
				}
			}
			else if(operation.equals("or")) {
				// check if at least one of the alarms happened in the given time window
				// if yes, ?? 
				int count = 0;
				for (String name : names) {
					for (Tuple tuple : tuples) {
						if (tuple.getDoubleByField("timestamp") >= System.currentTimeMillis() - time_window) {
							String this_name = tuple.getStringByField("reading_name") + "__"
								+ tuple.getStringByField("alarm_type");
							if (this_name == name) {
								count += 1;
							}
						}
					}
				}
				if (count >= 1) {
					collector.emit(new Values());
				}
			}
		}
		for (Tuple tuple : new_tuples) {
			String reading = tuple.getStringByField("reading_name") + "__" + tuple.getStringByField("alarm_type");
			// send alarms without aggregation to LogAlarm bolt
			if (!aggregated_readings.contains(reading)) {
				collector.emit(new Values());
			}
		}

	}

}
