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

import static com.mongodb.client.model.Filters.*;

public class Buffer extends BaseWindowedBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db;
	private Double last_emit = (double) System.currentTimeMillis();
    private InfluxDB influx_db;
    private String experiment_name = "pancake";
	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	    config_db = new ConfigDB(); 
        influx_db = InfluxDBFactory.connect("http://localhost:8086");
    }

	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuples = inputWindow.get();
		// Get time interval in ms for type from storm config db
        String type = tuples.get(0).getStringByField("topic");
        Double time_interval = 60000.0;
        try {

		    Document doc = config_db.read("settings", "experiment_config", eq("name", "storm"));
            Document time_intervals = (Document) doc.get("intervals");
            time_interval = time_intervals.getDouble(type) * 1000;
        } catch (Exception e) {
            
        }
        
		// only do this if last emit is one time_interval away
		if ((double) System.currentTimeMillis() - last_emit >= time_interval) {
            List<String> reading_names = new ArrayList<String>();
			List<String> host_per_reading = new ArrayList<String>();
			List<Double> value_per_reading = new ArrayList<Double>();

			// fill list of reading names in the window from last emit to now
			for (Tuple tuple : tuples) {
				if (tuple.getDoubleByField("timestamp") >= last_emit) {
                    String reading_name = tuple.getStringByField("reading_name");
                    if (!reading_names.contains(reading_name)) {
					reading_names.add(reading_name);
                    }
				}
			}
			// for each reading_name calculate mean of corresponding values, emit to stream
			// and fill list of value_per_reading
			for (String reading_name : reading_names) {
				List<Tuple> tuples_by_name = new ArrayList<Tuple>();
				for (Tuple tuple : tuples) {
					if (tuple.getDoubleByField("timestamp") - last_emit > 0 && tuple.getStringByField("reading_name").equals(reading_name)) {
						tuples_by_name.add(tuple);
					}
				}
				Double mean = 0.0;
				String host = tuples_by_name.get(0).getStringByField("host");
				for (Tuple tuple_by_name : tuples_by_name) {
                    mean += Double.parseDouble(tuple_by_name.getStringByField("value"));
                }
   				mean = mean / tuples_by_name.size();
				collector.emit(new Values(type, 
                            tuples_by_name.get(tuples_by_name.size() - 1).getDoubleByField("timestamp"),
                            host, reading_name, mean));
				
				host_per_reading.add(host);
				value_per_reading.add(mean);
            }
            
            if( reading_names.size() > 0){
			WriteToStorage(type, host_per_reading, reading_names, value_per_reading);
            }
			last_emit = (double) System.currentTimeMillis();
        }
    }

	private void WriteToStorage(String measurement, List<String> host_per_reading, List<String> rd_names,
			List<Double> values) {
		influx_db.setDatabase(experiment_name);
		Builder point = Point.measurement(measurement);
		for (int i = 0; i < values.size(); ++i) {
			point.addField(rd_names.get(i), values.get(i));
			if (host_per_reading.get(i) != "") {
				point.tag("host", host_per_reading.get(i));
			}
		}
		influx_db.write(point.build());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type", "timestamp", "host", "reading_name", "value"));

	}
}
