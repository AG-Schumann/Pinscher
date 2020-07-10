package Pinscher;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DifferentiatorBolt extends BaseWindowedBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {

		List<Tuple> tuples = inputWindow.get();
		Tuple tu = tuples.get(tuples.size() - 1);
		String topic = tu.getStringByField("topic");
		Double timestamp = tu.getDoubleByField("timestamp");
		String host = tu.getStringByField("host");
		String reading_name = tu.getStringByField("reading_name");
		Double value = tu.getDoubleByField("value");
		Double c = tu.getDoubleByField("c");
		Double setpoint = tu.getDoubleByField("setpoint");
		Double dt_diff = tu.getDoubleByField("dt_diff");
		Double recurrence = tu.getDoubleByField("recurrence");
		Double pid = tu.getDoubleByField("pid");
		double B = 0, C = 0, D = 0, E = 0, F = 0;
		List<Double> timestamps = new ArrayList<Double>();
		Double delta_t = tu.getDoubleByField("dt_diff");
		Double t0 = System.currentTimeMillis() - delta_t * 1000;
		List<Double> values = new ArrayList<Double>();
		for (Tuple tuple : tuples) {
			if (tuple.getStringByField("reading_name") == reading_name && tuple.getStringByField("host") == host) {
				Double t = tuple.getDoubleByField("timestamp");
				if (t >= t0) {
					timestamps.add(t);
					values.add(tuple.getDoubleByField("value"));
				}
			}
		}
		for (int i = 0; i < timestamps.size() - 1; ++i) {
			double t = (timestamps.get(i) - timestamps.get(0)) / 1000;
			double y = values.get(i);
			B += t * t;
			C += 1;
			D += t * y;
			E += y;
			F += t;
		}

		double slope = 0;
		if ((B * C - F * F) == 0) {
			// edge case, how to handle?
		} else {
			slope = (C * D - E * F) / (B * C - F * F);
		}
		pid += c * slope;
		collector.emit(new Values(topic, timestamp, host, reading_name, value, setpoint, tu.getValueByField("levels"),
				recurrence, pid));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value", "setpoint", "levels",
				"recurrence", "pid"));

	}
}
