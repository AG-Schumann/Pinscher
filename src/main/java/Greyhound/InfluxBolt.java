package stormcontrol;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private InfluxDB influxDB;
	private OutputCollector collector;
    private String experiment_name = "pancake"; 
	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		collector = outputCollector;
		influxDB = InfluxDBFactory.connect("http://localhost:8086");
		// this should be imported from settings.json in the future
		influxDB.setDatabase(experiment_name);
	}

	@Override
	public void execute(Tuple input) {
		String source = input.getSourceComponent();
        String reading_name = "";
        Double value = -1.;
        if (source.equals("PidBolt")) {
         reading_name = input.getStringByField("reading_name");
         value = input.getDoubleByField("pid");

        } else if (source.equals("KafkaSpout")) {
         reading_name = input.getStringByField("reading_name");
         value = Double.parseDouble(input.getStringByField("value"));
        } else {  
     
        reading_name = input.getString(1).split("__")[0];
        value = input.getDouble(0);
        }
		Point point = Point.measurement("alarms")
				.tag("source", source)
                .addField(reading_name, value)
				.build();
		influxDB.write(point);
		// System.out.println("WROTE POINT TO DATABASE" + sensor_name + " " + type + " @
		// " + new Date(input.getDoubleByField("timestamp").longValue()));
		collector.emit(input, new Values("end"));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("end"));
	}
}
