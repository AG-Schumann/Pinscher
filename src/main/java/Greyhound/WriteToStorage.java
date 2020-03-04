package Greyhound;

import static com.mongodb.client.model.Filters.eq;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDBException.DatabaseNotFoundException;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.Point.Builder;

public class WriteToStorage extends BaseRichBolt{
	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db;
	private InfluxDB influx_db;
	private String experiment_name = new String();
	private String mongo_uri = new String();
	
	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		
		this.collector = collector;
		experiment_name = (String) topoConf.get("EXPERIMENT_NAME");
		mongo_uri = (String) topoConf.get("MONGO_CONNECTION_URI");
		config_db = new ConfigDB(mongo_uri, experiment_name);
		String influx_server = (String) config_db.readOne("settings", "experiment_config", eq("name", "influx"))
				.get("server");
		influx_db = InfluxDBFactory.connect(influx_server);
	}

	@Override
	public void execute(Tuple input) {
		String topic = input.getStringByField("topic");
		Double value = input.getDoubleByField("value");
		Double timestamp = input.getDoubleByField("timestamp");
		Builder point = Point.measurement(topic).time(timestamp.longValue(), TimeUnit.MILLISECONDS);
		point.addField(input.getStringByField("reading_name"), value);
		String host = input.getStringByField("host");
		if (!host.equals("")) {
			point.tag("host", host);
		}
		if (topic.equals("sysmon")) {
			try {
				influx_db.setDatabase("common");
                        	influx_db.write(point.build());
                	} catch (DatabaseNotFoundException e) {
                        	influx_db.query(new Query("CREATE DATABASE common"));
                        	influx_db.setDatabase("common");
                        	influx_db.write(point.build());
			}
		} else {
			try {
				influx_db.setDatabase(experiment_name);
				influx_db.write(point.build());
			} catch (DatabaseNotFoundException e) {
				influx_db.query(new Query("CREATE DATABASE " + experiment_name));
				influx_db.setDatabase(experiment_name);
				influx_db.write(point.build());
			}
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
