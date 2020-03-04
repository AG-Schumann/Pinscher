package Greyhound;

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

public class IntegralBolt extends BaseWindowedBolt {

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
	public void execute(TupleWindow tuWindow) {

		List<Tuple> tuples = tuWindow.get();
		Tuple tu = tuples.get(tuples.size() - 1);
		String topic = tu.getStringByField("topic");
		Double timestamp = tu.getDoubleByField("timestamp");
		String host = tu.getStringByField("host");
		String reading_name = tu.getStringByField("reading_name");
		Double value = tu.getDoubleByField("value");
		Double B = tu.getDoubleByField("b");
		Double C = tu.getDoubleByField("c");
		Double setpoint = tu.getDoubleByField("setpoint");
		Double dt_int = tu.getDoubleByField("dt_int");
		Double dt_diff = tu.getDoubleByField("dt_diff");
		Double recurrence = tu.getDoubleByField("recurrence");
		Double pid = tu.getDoubleByField("pid");

		double integral = 0.0;
		// create lists of timestamps and values in window from now to now - delta_t
		List<Double> timestamps = new ArrayList<Double>();
		List<Double> values = new ArrayList<Double>();
		Double t0 = System.currentTimeMillis() - dt_int * 1000;
		for (Tuple tuple : tuples) {
			if (tuple.getStringByField("reading_name") == reading_name && tuple.getStringByField("host") == host) {
				Double t = tuple.getDoubleByField("timestamp");
				if (t >= t0) {
					timestamps.add(t / 1000);
					values.add(tuple.getDoubleByField("value") - setpoint);
				}
			}
		}

		if (timestamps.size() > 0 && values.size() > 0) {
			// integral from t0 to first data point in window
			integral += (timestamps.get(0) - t0 / 1000) * values.get(0);

			// integral over the data points (trapezoidal rule)
			for (int i = 0; i < timestamps.size() - 1; ++i) {
				integral += 0.5 * (timestamps.get(i + 1) - timestamps.get(i)) * (values.get(i + 1) + values.get(i));
			}
			pid += B * integral;
			collector.emit(new Values(topic, timestamp, host, reading_name, value, C, setpoint, dt_diff,
					tu.getValueByField("levels"), recurrence, pid));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "value", "c", "setpoint", "dt_diff",
				"levels", "recurrence", "pid"));
	}
}
