package Greyhound;

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
        double time_since = 0.0;
		List<Double> timestamps = new ArrayList<Double>();
		List<Double> values = new ArrayList<Double>();
		double lower_threshold = tu.getDoubleByField("lower_threshold");
		double upper_threshold = tu.getDoubleByField("upper_threshold");
		for (Tuple tuple : tuples) {
			timestamps.add(tuple.getDoubleByField("timestamp"));
			values.add(tuple.getDoubleByField("value"));
		}
		int last_in_threshold = 0;
		for (int i = 0 ; i < values.size(); ++i) {
			if (values.get(i) >= lower_threshold && values.get(i) <= upper_threshold) {
				last_in_threshold = i;
			}
           
        }
		time_since = (System.currentTimeMillis() - timestamps.get(last_in_threshold))/1000;
		collector.emit(new Values(tu.getStringByField("topic"),
				tu.getDoubleByField("timestamp"), tu.getStringByField("host"),
				tu.getStringByField("reading_name"), time_since, lower_threshold, upper_threshold, 
                tu.getValueByField("max_duration")));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "time_since",
                    "lower_threshold", "upper_threshold", "max_duration"));
	}
}
