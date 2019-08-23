package doberman.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class TimeSinceBolt extends BaseWindowedBolt {
	
	private OutputCollector collector;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}
	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuples = inputWindow.get();
		List<Double> timestamps = new ArrayList<Double>();
		List<Double> values = new ArrayList<Double>();
		double lower_threshold = tuples.get(tuples.size() - 1).getDoubleByField("lower_threshold");
		double upper_threshold = tuples.get(tuples.size() - 1).getDoubleByField("upper_threshold");
		for (Tuple tuple : tuples) {
			timestamps.add(tuple.getDoubleByField("timestamp"));
			values.add(tuple.getDoubleByField("value"));
		}
		int last_in_threshold = 0;
		for (int i = 0 ; i < tuples.size(); i++) {
			if (values.get(i) > lower_threshold && values.get(i) < upper_threshold) {
				last_in_threshold = i;
			}
		double time_since = System.currentTimeMillis() - timestamps.get(last_in_threshold);
		collector.emit(new Values(tuples.get(0).getStringByField("topic"),
				tuples.get(tuples.size() - 1).getDoubleByField("timestamp"), time_since, "time_since"));
		}
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "value", "type"));

	}
	

}
