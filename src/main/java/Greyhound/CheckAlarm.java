package Greyhound;

import java.util.ArrayList;
import java.util.Date;
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
import org.bson.Document;

public class CheckAlarm extends BaseWindowedBolt {

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
		String source = tu.getSourceComponent();
		if (source.equals("PidBolt")) {
			List<Double> ret = check_pid(tuples);
			double howBad = ret.get(0);
			if (howBad > -1.) {
				collector.emit(new Values());
			}
		} else if (source.contentEquals("TimeSinceBolt")) {
			List<Double> ret = check_timesince(tuples);
			double howBad = ret.get(0);
			if (howBad > -1.) {
				collector.emit(new Values(tu.getStri));
			}
		}
	}

	private List<Double> check_timesince(List<Tuple> tuples) {
		/*
		 * Checks for Time since alarms. 
		 * Returns: List of howBad and corresponding value of max_duration, if howBad >= 0. 
		 */
		List<Double> ret = new ArrayList<Double>();
		Double howBad = -1.;
		Tuple tu = tuples.get(tuples.size() - 1);
		double time_since = tu.getDoubleByField("time_since");
		List<Double> max_duration = new ArrayList<Double>();
		try {
			// one alarm level ("max_duration" : <Double>)
			Double maxd = tu.getDoubleByField("max_duration");
			max_duration.add(maxd);
		} catch (ClassCastException e) {
			try {
				// multiple alarm levels ("max_duration" : [<Double>, <Double>, ...])
				max_duration = (List<Double>) tu.getValueByField("max_duration");
			} catch (Exception ee) {
				ret.add(-2.0);
				return ret;
			}
		}
		for (int i = 0; i < max_duration.size(); ++i) {
			if (time_since > max_duration.get(i)) {
				howBad += 1.;
			}
		}
		ret.add(howBad);
		if (howBad > -1.) {
			ret.add(max_duration.get(howBad.intValue()));
		}
		return ret;
	}

	private List<Double> check_pid(List<Tuple> tuples) {
		/*
		 * returns howBad
		 * 
		 * howBad = -1 : no Alarm howBad = 0 : alarm of level 0 howBad = 1 : alarm of
		 * level 1 ... Define levels at some point (in Doberman)
		 * 
		 */
		List<Double> ret = new ArrayList<Double>();
		Double howBad = -1.;
		Tuple tu = tuples.get(tuples.size() - 1);
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
			ret.add(-2.0);
			return ret;
		}
		for (int j = 0; j < lower_threshold.size(); ++j) {
			int recurrence = 0;
			// check if values are iniside threshold of level j for tuples from newest to
			// oldest
			for (int i = tuples.size() - 1; i >= 0; --i) {
				Double pid = tuples.get(i).getDoubleByField("pid");
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
		ret.add(howBad);
		if (howBad > -1.) {
			ret.add(lower_threshold.get(howBad.intValue()));
			ret.add(upper_threshold.get(howBad.intValue()));
		}
		return ret;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(
				new Fields("topic", "timestamp", "host", "reading_name", "alarm_type", "additional_parameters"));
	}
}
