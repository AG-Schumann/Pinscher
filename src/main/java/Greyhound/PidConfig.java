package Greyhound;

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
	public void prepare(Map<String, Object> topoConf, TopologyContext topologyContext,
			OutputCollector collector) {
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
		String key = reading_name + "_" + host + "_" + timestamp;
	
		// get PID alarm parameter from config DB
		try {
        Document doc = new Document();
		if (host.equals("")) {
			doc = config_db.readOne(db_name, "readings", eq("name", reading_name));
		} else {
			doc = config_db.readOne(db_name, "readings", and(eq("name", reading_name), eq("host", host)));
		}
        String runmode = doc.getString("runmode");
	List<Document> alarms = (List<Document>) doc.get("alarms");
		for (Document alarm : alarms) {
			if (alarm.getString("type").equals("pid") &&
					alarm.getString("enabled").equals("true") &&
					runmode.equals("default")) {
				collector.emit(new Values(topic, timestamp, host, reading_name,
						input.getDoubleByField("value"), alarm.getDouble("a"), alarm.getDouble("b"),
						alarm.getDouble("c"), alarm.getDouble("setpoint"),
						alarm.getDouble("dt_int"), alarm.getDouble("dt_diff"),
						alarm.get("levels"), alarm.getDouble("recurrence"), key));
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
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value", "a", "b", "c",
				"setpoint", "dt_int", "dt_diff", "levels", "recurrence", "key"));

	}
}
