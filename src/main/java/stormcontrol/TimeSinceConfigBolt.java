package stormcontrol;

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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TimeSinceConfigBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private MongoCollection<Document> collection;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

		String experiment_name = "testing";
		String db_name = experiment_name + "_settings";
		ConnectionString connection_string = new ConnectionString(
				"mongodb://webmonitor:42RKBu2QyeOUHkxOdHAhjfIpw1cgIQVgViO4U4nPr0s=@10.4.73.172:27010/admin");
		MongoClientSettings settings = MongoClientSettings.builder()
				.applyConnectionString(connection_string).retryWrites(true).build();
		MongoClient mongoClient = MongoClients.create(settings);
		MongoDatabase database = mongoClient.getDatabase(db_name);
		collection = database.getCollection("readings");
	}

	@Override
	public void execute(Tuple input) {
		Document doc = collection.find(eq("key", input.getStringByField("topic"))).first();
		String quantity =(String)doc.get("quantity");
        List<Document> alarms = (List<Document>) doc.get("alarms");
		for (Document alarm : alarms) {
			if (alarm.getString("type").equals("time_since")) {
				collector.emit(new Values(input.getStringByField("topic"),
                            input.getDoubleByField("timestamp"),
                            input.getDoubleByField("value"),
                            quantity,
                            alarm.getDouble("lower_threshold"),
                            alarm.getDouble("upper_threshold"),
                            alarm.getDouble("max_duration")));
			}
		}
		collector.ack(input);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "value", "quantity", "lower_threshold",
                    "upper_threshold", "max_duration"));
	}
}
