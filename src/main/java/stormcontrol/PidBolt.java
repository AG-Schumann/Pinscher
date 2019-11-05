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

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	@Override
	public void prepare(Map<String, Object> map, TopologyContext topologyContext,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {

		Double A = input.getDoubleByField("a");
		Double B = input.getDoubleByField("b");
		Double C = input.getDoubleByField("c");
		Double integral = input.getDoubleByField("integral");
		Double proportional = input.getDoubleByField("proportional");
		Double derivative = input.getDoubleByField("derivative");
		Double pid = A * integral + B * proportional + C * derivative;
		collector.emit(new Values(input.getStringByField("topic"), input.getDoubleByField("timestamp"),
				input.getStringByField("host"), input.getStringByField("reading_name"), 
				input.getDoubleByField("recurrence"), input.getValueByField("levels"), pid));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "recurrence", "levels", "pid"));

	}
}
