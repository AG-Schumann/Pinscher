package stormcontrol;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DifferentiatorBolt extends BaseWindowedBolt {

	private OutputCollector collector;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, 
            OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {

		List<Tuple> tuples = inputWindow.get();
		double B = 0, C = 0, D = 0, E = 0, F = 0;
		List<Double> timestamps = new ArrayList<Double>();
        Double delta_t = tuples.get(tuples.size() - 1).getDoubleByField("dt_diff");
		Double t0 = System.currentTimeMillis() - delta_t * 1000;
		for (Tuple tuple : tuples) {
			Double t = tuple.getDoubleByField("timestamp");
			if (t > t0) {
				timestamps.add(tuple.getDouble(1));
			}
		}
		double t_min = Collections.min(timestamps);
		for (Tuple tuple : tuples) {
			double t = (tuple.getDoubleByField("timestamp") - t_min) / 1000;
			double y = tuple.getDoubleByField("value");
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
		collector.emit(new Values(tuples.get(0).getStringByField("topic"),
				tuples.get(tuples.size() - 1).getDoubleByField("timestamp"), slope, "derivative",
				tuples.get(tuples.size() - 1).getStringByField("key")));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "derivative", "type", "key"));

	}
}
