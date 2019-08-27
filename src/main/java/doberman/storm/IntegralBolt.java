package doberman.storm;

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

	private OutputCollector collector;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
            OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {

		List<Tuple> tuples = inputWindow.get();
        System.out.println("TUPLES: " + tuples);
		Double setpoint = tuples.get(tuples.size() - 1).getDoubleByField("setpoint");
        double integral = 0.0;
		// create lists of timestamps and values in window from now to now - delta_t
		List<Double> timestamps = new ArrayList<Double>();
		List<Double> values = new ArrayList<Double>();
        Double delta_t = tuples.get(tuples.size() - 1).getDoubleByField("dt_int");
		Double t0 = System.currentTimeMillis() - delta_t * 1000;
		for (Tuple tuple : tuples) {
			Double t = tuple.getDoubleByField("timestamp");
			if (t >= t0) {
				timestamps.add(t / 1000);
				values.add(tuple.getDoubleByField("value") - setpoint);
			}
		}
        System.out.println("TIMESTAMPS: " + timestamps);

		// integral from t0 to first data point in window
		integral += (timestamps.get(0) - t0 / 1000) * values.get(0);

		// integral over the data points (trapezoidal rule)
		for (int i = 0; i < tuples.size() - 1; ++i) {
			integral += 0.5 * (timestamps.get(i + 1) - timestamps.get(i))
					* (values.get(i + 1) + values.get(i));
		}
		System.out.println("INTEGRAL: " + integral);
        collector.emit(new Values(tuples.get(0).getStringByField("topic"),
				tuples.get(tuples.size() - 1).getDoubleByField("timestamp"), integral,
				"integral", tuples.get(tuples.size() - 1).getStringByField("key")));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "integral", "type", "key"));
	}
}
