package stormcontrol;

import static com.mongodb.client.model.Filters.*;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

public class TimeSinceConfig extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db = new ConfigDB();
	private String db_name = "testing";

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String type = input.getStringByField("type");
		Double timestamp = input.getDoubleByField("timestamp");
		String host = input.getStringByField("host");
		String reading_name = input.getStringByField("reading_name");

		Document doc = new Document();
		if (host == "") {
			doc = config_db.read(db_name, "readings", eq("name", reading_name));
		} else {
			doc = config_db.read(db_name, "readings", and(eq("name", reading_name), eq("host", host)));
		}
		List<Document> alarms = (List<Document>) doc.get("alarms");
		for (Document alarm : alarms) {
			if (alarm.getString("type").equals("time_since")) {
				collector.emit(new Values(type, timestamp, host, reading_name,
						input.getDoubleByField("value"), alarm.getDouble("lower_threshold"),
						alarm.getDouble("upper_threshold"), alarm.get("max_duration")));
			}
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type", "timestamp", "host", "reading_name", "value",
				"lower_threshold", "upper_threshold", "max_duration"));
	}
}
