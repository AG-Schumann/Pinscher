package doberman.storm;

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

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxBolt extends BaseRichBolt {

	private InfluxDB influxDB;
	private OutputCollector collector;

	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		collector = outputCollector;
		influxDB = InfluxDBFactory.connect("http://localhost:8086");
		// this should be imported from settings.json in the future
		influxDB.setDatabase("testing_data");
	}

	@Override
	public void execute(Tuple input) {

		String topic = input.getStringByField("topic");
		String[] parts = topic.split("__");
		String sensor_name = parts[0];
		String reading = parts[1];
        String quantity = input.getStringByField("quantity");
		Point point = Point.measurement(quantity)
				.time(input.getDoubleByField("timestamp").longValue(), TimeUnit.MILLISECONDS)
				.tag("sensor_name", sensor_name).addField(reading, input.getDouble(2))
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