package Pinscher;

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
		String experiment_name = (String) topoConf.get("EXPERIMENT_NAME");
		String mongo_uri = (String) topoConf.get("MONGO_CONNECTION_URI");
		config_db = new ConfigDB(mongo_uri, experiment_name);
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
			String runmode = doc.getString("runmode");
			for (Document alarm : alarms) {
				if (alarm.getString("type").equals("simple") && 
						alarm.getString("enabled").equals("true") &&
						runmode.equals("default")) {
			
					collector.emit(new Values(topic, timestamp, host, reading_name, value, alarm.getDouble("setpoint"),
								alarm.get("levels"), alarm.getDouble("recurrence")));
				}
			}
		} catch (Exception e) {
			String msg = "Can't access alarm config for " + reading_name; 
            		config_db.log(msg, 20);
		} finally {
			collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value", "setpoint", "levels", "recurrence"));
	}

}
