package stormcontrol;


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
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Buffer extends BaseWindowedBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private MongoCollection<Document> collection;
	private Double last_emit = (double) System.currentTimeMillis();

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		String experiment_name = "testing";
		String db_name = experiment_name + "_storm";
		ConnectionString connection_string = new ConnectionString(
				"mongodb://webmonitor:42RKBu2QyeOUHkxOdHAhjfIpw1cgIQVgViO4U4nPr0s=@10.4.73.172:27010/admin");
		MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connection_string)
				.retryWrites(true).build();
		MongoClient mongoClient = MongoClients.create(settings);
		MongoDatabase database = mongoClient.getDatabase(db_name);
		collection = database.getCollection("readout_interval");
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuples = inputWindow.get();
		Document doc = collection.find().first();
		String type = tuples.get(0).getStringByField("type");
		Double time_interval = doc.getDouble(type);

		if (System.currentTimeMillis() - last_emit >= time_interval) {

			List<String> reading_names = new ArrayList<String>();
			List<Double> value_per_reading = new ArrayList<Double>();
			for (Tuple tuple : tuples) {
				reading_names.add(tuple.getStringByField("reading_name"));
			}
			for (String reading_name : reading_names) {
				List<Tuple> tuples_by_name = new ArrayList<Tuple>();
				for (Tuple tuple : tuples) {
					if (tuple.getStringByField("reading_name") == reading_name) {
						tuples_by_name.add(tuple);
					}
				}
				Double mean = 0.0;
				for (Tuple tuple : tuples_by_name) {
					mean += tuple.getDoubleByField("value");
				}
				if (tuples_by_name.size() > 0) {
					mean = mean / tuples_by_name.size();
					collector.emit(new Values(tuples_by_name.get(0).getStringByField("type"),
							tuples_by_name.get(tuples_by_name.size() - 1).getDoubleByField("timestamp"),
							reading_name, mean));
				}
				value_per_reading.add(mean);
			}
		WriteToStorage(type,reading_names, value_per_reading);
		}
	}

	private void WriteToStorage(String measurement, List<String> rd_names, List<Double> values) {

		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		influxDB.setDatabase("testing_data");
		Builder point = Point.measurement(measurement);
		for (int i=0; i<rd_names.size(); ++i) {
			point.addField(rd_names.get(i), values.get(i));
		}		
		influxDB.write(point.build());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type", "timestamp", "reading_name", "value"));

	}
}
