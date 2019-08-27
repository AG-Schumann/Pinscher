package doberman.storm;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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

import static com.mongodb.client.model.Filters.eq;

public class ConfigBolt extends BaseRichBolt {

	private OutputCollector collector;
	private MongoCollection<Document> collection;

	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext,
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
		String key = input.getStringByField("topic") + "_" + input.getDoubleByField("timestamp");
		for (Document alarm : alarms) {
			if (alarm.getString("type").equals("pid")) {
				collector.emit(new Values(input.getString(0), input.getDouble(1), input.getDouble(2),
                        quantity,alarm.getDouble("a"), alarm.getDouble("b"), alarm.getDouble("c"),
						alarm.getDouble("setpoint"), alarm.getDouble("delta_t_integral"),
						alarm.getDouble("delta_t_differential"), alarm.getDouble("lower_threshold"),
						alarm.getDouble("upper_threshold"), key));
			}
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "value", "quantity",  "a", "b", "c", "setpoint",
                    "dt_int","dt_diff", "lower_threshold", "upper_threshold", "key"));

	}
}
