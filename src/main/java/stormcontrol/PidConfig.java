package stormcontrol;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import java.util.List;
import java.util.Map;
import static com.mongodb.client.model.Filters.*;

public class PidConfig extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db;
	private String db_name = "settings";

	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext,
			OutputCollector collector) {
		this.collector = collector;
        config_db = new ConfigDB();
	}

	@Override
	public void execute(Tuple input) {

		String type = input.getStringByField("type");
		Double timestamp = input.getDoubleByField("timestamp");
		String host = input.getStringByField("host");
		String reading_name = input.getStringByField("reading_name");
		String key = reading_name + "_" + host + "_" + timestamp;

		// get PID alarm parameter from config DB
		Document doc = new Document();
		if (host.equals("")) {
			doc = config_db.read(db_name, "readings", eq("name", reading_name));
		} else {
			doc = config_db.read(db_name, "readings", and(eq("name", reading_name), eq("host", host)));
		}
		List<Document> alarms = (List<Document>) doc.get("alarms");
		for (Document alarm : alarms) {
			if (alarm.getString("type").equals("pid")) {
				collector.emit(new Values(type, timestamp, host, reading_name,
						input.getDoubleByField("value"), alarm.getDouble("a"), alarm.getDouble("b"),
						alarm.getDouble("c"), alarm.getDouble("setpoint"),
						alarm.getDouble("dt_integral"), alarm.getDouble("dt_differential"),
						alarm.get("levels"), alarm.getDouble("recurrence"), key));
			}
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type", "timestamp", "host", "reading_name", "value", "a", "b", "c",
				"setpoint", "dt_int", "dt_diff", "levels", "recurrence", "key"));

	}
}
