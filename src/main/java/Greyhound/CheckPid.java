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

public class CheckPid extends BaseWindowedBolt {

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
		// create a list of all tuples with same reading_name and host as the most recent tuple tu.
		String this_reading_name = tu.getStringByField("reading_name");
		String this_host = tu.getStringByField("host");
		List<Tuple> these_tuples = new ArrayList<Tuple>();
		for (Tuple tuple : tuples) {
			if( tuple.getStringByField("reading_name").equals(this_reading_name) &&
					tuple.getStringByField("host").equals(this_host)) {
				these_tuples.add(tuple);
			}
		}
		Double howBad = -1.;
		double max_recurrence = tu.getDoubleByField("recurrence");
		List<Double> lower_threshold = new ArrayList<Double>();
		List<Double> upper_threshold = new ArrayList<Double>();
		List<?> levels = (List<?>) tu.getValueByField("levels");
		// one alarm level [<Double>, <Double>]
		if (levels.get(0) instanceof Double) {
			lower_threshold.add((Double) levels.get(0));
			upper_threshold.add((Double) levels.get(1));
			// multiple alarm levels [[<Double>, <Double>], [<Double>, <Double>], ...]
		} else if (levels.get(0) instanceof List<?>) {
			for (int i = 0; i < levels.size(); ++i) {
				lower_threshold.add(((List<Double>) levels.get(i)).get(0));
				upper_threshold.add(((List<Double>) levels.get(i)).get(1));
				// sort lists here or expect correct entries?
			}
		} else {
			// ???
		}
		for (int j = 0; j < lower_threshold.size(); ++j) {
			int recurrence = 0;
			// check if values are iniside threshold of level j for tuples from newest to
			// oldest
			for (int i = these_tuples.size() - 1; i >= 0; --i) {
				Double pid = these_tuples.get(i).getDoubleByField("pid");
				if (pid < lower_threshold.get(j) || pid > upper_threshold.get(j)) {
					recurrence += 1;
					if (recurrence >= max_recurrence) {
						howBad += 1.;
						break;
					}
				} else {
					break;
				}
			}
		}
		if (howBad > -1.) {
			List<Double> additional_parameters = new ArrayList<Double>();
			collector.emit(new Values(tu.getStringByField("topic"), tu.getDoubleByField("timestamp"),
					tu.getStringByField("host"), tu.getStringByField("reading_name"), "pid", howBad, additional_parameters));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(
				new Fields("topic", "timestamp", "host", "reading_name", "alarm_type", "howBad", "additional_parameters"));
	}

}
