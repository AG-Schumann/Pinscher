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

		// Get time interval in ms for type from storm config db
		Document doc = collection.find().first();
		String type = tuples.get(0).getStringByField("type");
		Double time_interval = doc.getDouble(type) * 1000;

		// only do this if last emit is one time_interval away
		if ((double) System.currentTimeMillis() - last_emit >= time_interval) {

			List<String> reading_names = new ArrayList<String>();
			List<String> host_per_reading = new ArrayList<String>();
			List<Double> value_per_reading = new ArrayList<Double>();

			// fill list of reading names in the window from last emit to now
			for (Tuple tuple : tuples) {
				if (tuple.getDoubleByField("timestamp") >= last_emit) {
					reading_names.add(tuple.getStringByField("reading_name"));
				}
			}
			// for each reading_name calculate mean of corresponding values, emit to stream
			// and
			// fill list of value_per _reading
			for (String reading_name : reading_names) {
				List<Tuple> tuples_by_name = new ArrayList<Tuple>();
				for (Tuple tuple : tuples) {
					if (tuple.getStringByField("reading_name") == reading_name
							&& tuple.getDoubleByField("timestamp") >= last_emit) {
						tuples_by_name.add(tuple);
					}
				}
				Double mean = 0.0;
				String host = tuples_by_name.get(0).getStringByField("host");
				if (tuples_by_name.size() > 0) {
					for (Tuple tuple_by_name : tuples_by_name) {
						mean += tuple_by_name.getDoubleByField("value");

						mean = mean / tuples_by_name.size();

						collector.emit(new Values(type,
								tuples_by_name.get(tuples_by_name.size() - 1).getDoubleByField("timestamp"),
								host, reading_name, mean));
					}
				}
				host_per_reading.add(host);
				value_per_reading.add(mean);
			}
			WriteToStorage(type, host_per_reading, reading_names, value_per_reading);
			last_emit = (double) System.currentTimeMillis();
		}
	}

	private void WriteToStorage(String measurement, List<String> host_per_reading, List<String> rd_names,
			List<Double> values) {

		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086");
		influxDB.setDatabase("testing_data");
		Builder point = Point.measurement(measurement);
		for (int i = 0; i < rd_names.size(); ++i) {
			point.addField(rd_names.get(i), values.get(i));
			if (host_per_reading.get(i) != "") {
				point.tag("host", host_per_reading.get(i));
			}
		}
		influxDB.write(point.build());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type", "timestamp", "host", "reading_name", "value"));

	}
}
