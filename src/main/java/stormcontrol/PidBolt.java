package stormcontrol;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PidBolt extends BaseRichBolt {

	private OutputCollector collector;

	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {

		Double A = input.getDouble(2);
		Double B = input.getDouble(3);
		Double C = input.getDouble(4);
		Double integral = input.getDouble(7);
		Double proportional = input.getDouble(8);
		Double derivative = input.getDouble(9);
		Double pid = A * integral + B * proportional + C * derivative;
		collector.emit(new Values(input.getString(0), input.getDouble(1), pid, "pid"));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "pid", "type"));

	}
}
