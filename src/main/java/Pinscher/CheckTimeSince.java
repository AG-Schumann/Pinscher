package Pinscher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CheckTimeSince extends BaseRichBolt {
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
	public void execute(Tuple input) {
		Double howBad = -1.;
		double time_since = input.getDoubleByField("time_since");
		List<Double> max_duration = new ArrayList<Double>();
		try {
			// one alarm level ("max_duration" : <Double>)
			Double maxd = input.getDoubleByField("max_duration");
			max_duration.add(maxd);
		} catch (ClassCastException e) {
			try {
				// multiple alarm levels ("max_duration" : [<Double>, <Double>, ...])
				max_duration = (List<Double>) input.getValueByField("max_duration");
			} catch (Exception ee) {
				// ??
			}
		}
		for (int i = 0; i < max_duration.size(); ++i) {
			if (time_since > max_duration.get(i)) {
				howBad += 1.;
			}
		}
		if (howBad > -1.) {
			List<Double> additional_parameters = new ArrayList<Double>();
			additional_parameters.add(input.getDoubleByField("time_since"));
			additional_parameters.add(input.getDoubleByField("lower_threshold"));
			additional_parameters.add(input.getDoubleByField("upper_threshold"));
			additional_parameters.add(max_duration.get(howBad.intValue()));
			collector.emit(new Values(input.getStringByField("topic"), input.getDoubleByField("timestamp"),
					input.getStringByField("host"), input.getStringByField("reading_name"), "timesince", howBad,
					additional_parameters));
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "timestamp", "host", "reading_name", "alarm_type", "howBad",
				"additional_parameters"));

	}

}
