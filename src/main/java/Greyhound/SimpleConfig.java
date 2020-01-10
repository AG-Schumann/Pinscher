package Greyhound;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

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

public class SimpleConfig extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db;
	private String db_name = "settings";

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		config_db = new ConfigDB();
	}

	@Override
	public void execute(Tuple input) {

		String topic = input.getStringByField("topic");
		Double timestamp = input.getDoubleByField("timestamp");
		String host = input.getStringByField("host");
		String reading_name = input.getStringByField("reading_name");
		Double value = input.getDoubleByField("value");
		// get simple alarm parameter from config DB
		try {
			Document doc = new Document();
			if (host.equals("")) {
				doc = config_db.readOne(db_name, "readings", eq("name", reading_name));
			} else {
				doc = config_db.readOne(db_name, "readings", and(eq("name", reading_name), eq("host", host)));
			}
			List<Document> alarms = (List<Document>) doc.get("alarms");
			for (Document alarm : alarms) {
				if (alarm.getString("type").equals("simple")) {
					collector.emit(new Values(topic, timestamp, host, reading_name, value,
							alarm.get("levels"), alarm.getDouble("recurrence")));
				}
			}
		} catch (Exception e) {
			System.out.println("SOMETHING WENT WRONG");
		} finally {
			collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value", "levels", "recurrence"));
	}

}
