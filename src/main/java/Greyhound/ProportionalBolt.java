package Greyhound;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ProportionalBolt extends BaseRichBolt {

	/**
	 * First step of pid calculation. Calculates pid = A*(Value - Setpoint)
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String topic = input.getStringByField("topic");
		Double timestamp = input.getDoubleByField("timestamp");
		String host = input.getStringByField("host");
		String reading_name = input.getStringByField("reading_name");
		Double value = input.getDoubleByField("value");
		Double A = input.getDoubleByField("a");
		Double B = input.getDoubleByField("b");
		Double C = input.getDoubleByField("c");
		Double setpoint = input.getDoubleByField("setpoint");
		Double dt_int = input.getDoubleByField("dt_int");
		Double dt_diff = input.getDoubleByField("dt_diff");
		Double recurrence = input.getDoubleByField("recurrence");
		Double pid = A * (value - setpoint);
		collector.emit(new Values(topic, timestamp, host, reading_name, value, B, C, setpoint, dt_int, dt_diff,
				input.getValueByField("levels"), recurrence, pid));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value", "b", "c", "setpoint",
				"dt_int", "dt_diff", "levels", "recurrence", "pid"));

	}
}
