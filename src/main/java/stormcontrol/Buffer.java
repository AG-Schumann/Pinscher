package stormcontrol;
import javax.script.ScriptEngineManager;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;
import org.bson.Document;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Query;
import org.influxdb.InfluxDBException.DatabaseNotFoundException;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.*;

public class Buffer extends BaseWindowedBolt {
	/**
     * Each buffer recieves tuples from exactly one topic. It fetches the defined
	 * commit interval from the config DB for its topic and collects tuples as long
	 * as the last commit was less then one commit interval ago. When the last
	 * commit is more than one commit interval ago, Buffer calculates the mean of
	 * the collected tuples, emits each reading downstream and writes all points to
	 * the storage DB.
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
        String topic = tuples.get(0).getStringByField("topic");
        Double commit_interval = getCommitInterval(topic);
        Double min_readout_interval = getMinReadoutInterval(topic);
        // try to prevent weird stuff from happening...
        if (min_readout_interval.equals(commit_interval)) {
            min_readout_interval = 0.0;
        }
		// only do this if last emit is more than one commit interval - minimal readout interval  away
		if ((double) System.currentTimeMillis() - last_emit >= commit_interval - min_readout_interval) {
            // create a map of keys (<reading_name>,<host>) and the corresponding tuples
            Map<String,List<Tuple>> map = new HashMap<String,List<Tuple>>();
			for (Tuple tuple : tuples) {
				if (tuple.getDoubleByField("timestamp") >= last_emit) {
                    String key = tuple.getStringByField("reading_name") +","+ tuple.getStringByField("host");
                    if (!map.containsKey(key)) {
                        map.put(key, new ArrayList<Tuple>());
                    }
                    map.get(key).add(tuple);
                }
            }
            // calculate mean value of all tuples for each key
            Map<String, Double> results = new HashMap<String, Double>();
            for (String this_key : map.keySet()) {
                double mean = .0;
                for(Tuple tuple : map.get(this_key)) {
                    mean += Double.valueOf(tuple.getStringByField("value"));
                }
                mean /= map.get(this_key).size();
                results.put(this_key, mean);

            }
            results = addCombinedReadings(topic, results);

            // wait until last commit is "exactly" one commit interval away
            while((double) System.currentTimeMillis() - last_emit < commit_interval) {
                Utils.sleep(min_readout_interval.longValue() / 100);
            }
            if(results.size() > 0) {
			    WriteToStorage(topic, results);
            }
			last_emit = (double) System.currentTimeMillis();
            for(String key : results.keySet()) {
                collector.emit(new Values(topic, last_emit, key.split(",",2)[1],key.split(",",2)[0],
                            results.get(key))); 
            }
        }
    }

    private Map<String, Double> addCombinedReadings(String topic, Map<String, Double> results) {
            
            List<Document> cobined = config_db.readMany("settings", "readings", 
                    and(eq("sensor", "storm"), eq("topic", topic)));
            for (Document combined_reading : cobined) {
                String operation = (String) combined_reading.get("operation");
                // replace names of single_readings with the current (mean) value
                // operation can either contain '<reading_name>,<host>' or just '<reading_name>' for 
                // readings without specified host
                for(HashMap.Entry<String, Double> result : results.entrySet()) {
                    String name = result.getKey();
                    if (name.endsWith(",")) {
                        name = name.replace(",","");
                    }
                    operation = operation.replace(name, Double.toString(result.getValue()));
                }
                Double this_result = 0.0;
                try {
                    ScriptEngineManager mgr = new ScriptEngineManager();
                    ScriptEngine engine = mgr.getEngineByName("JavaScript");
                    this_result = (Double) engine.eval(operation);
                    results.put((String) combined_reading.get("name") + ",", this_result);

                } catch (ScriptException e) {
                    //log message
                }
            }
            return results;
    }

	private void WriteToStorage(String topic, Map<String, Double> results) {
        Builder point = Point.measurement(topic);
		for (String key : results.keySet()) {
			String[] id = key.split(",", -1); 
            point.addField(id[0], results.get(key));
			String host = id[1];
            if (!host.equals("")) {
			    point.tag("host", host);
			}
		}
        try {
            influx_db.setDatabase(experiment_name);
		    influx_db.write(point.build());
        } catch (DatabaseNotFoundException e) {
            influx_db.query(new Query("CREATE DATABASE " + experiment_name));
            influx_db.setDatabase(experiment_name);
            influx_db.write(point.build());
        }
	}
    
    private double getCommitInterval(String topic) {
        try {
		    Document doc = config_db.readOne("settings", "experiment_config", eq("name", "storm"));
            Document intervals = (Document) doc.get("intervals");
            return intervals.getDouble(topic) * 1000;
        } catch (Exception e) {
            return 60_000;
        }
    }
    
    private double getMinReadoutInterval(String topic){
        try {
            List<Document> docs = config_db.readMany("settings", "readings", eq("topic", topic));
            List<Double> intervals = new ArrayList<Double>();
            for (Document doc : docs) {
                intervals.add((Double) doc.get("readout_interval"));
            }
            return Collections.min(intervals) * 1000.0;
        } catch (Exception e) {
        }   
        return .0;
    }

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value"));
	}
}
