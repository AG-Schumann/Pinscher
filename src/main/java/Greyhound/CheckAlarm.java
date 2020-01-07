package Greyhound;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.bson.Document;

public class CheckAlarm extends BaseWindowedBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
    private ConfigDB config_db;
	private String db_name = "logging";

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;        
	    config_db = new ConfigDB();
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuples = inputWindow.get();
		Tuple tu = tuples.get(tuples.size() - 1);
		String source = tu.getSourceComponent();
		boolean hasHost = false;
		String msg = new String();
		if (source.equals("PidBolt")) {
			List<Double> ret = check_pid(tuples);
			double howBad = ret.get(0);
			if (howBad > -1.) {
				double lower_threshold = ret.get(1);
				double upper_threshold = ret.get(2);
                double pid = tu.getDoubleByField("pid");
				// deal with sysmon and host, include type
				String host = tu.getStringByField("host");
				if (!host.equals("")) {
					hasHost = true;
				}
				msg = String.format(
                        "Pid alarm for %s measurement %s%s: %.3f is outside alarm range (%.3f, %.3f)",
						tu.getStringByField("topic"), tu.getStringByField("reading_name"),
                        hasHost ? " of " + host : "", pid, lower_threshold, upper_threshold);
		        Document log = new Document();
                log.put("when", new Date(tu.getDoubleByField("timestamp").longValue()));
                log.put("howbad", howBad);
                log.put("msg", msg);
                config_db.writeOne(db_name, "alarms", log);
			}
		} 
        else if (source.contentEquals("TimeSinceBolt")) {
			List<Double> ret = check_timesince(tuples);
			double howBad = ret.get(0);
			if (howBad > -1.) {
				double max_duration = ret.get(1);
				String host = tu.getStringByField("host");
				if (!host.equals("")) {
					hasHost = true;
				}
				// the input is not providing all these values at the moment
				msg = String.format(
						"TimeSince alarm for %s measurement %s%s: %.3f is outside alarm range (%.3f, %.3f) for more than %.0f seconds",
						tu.getStringByField("topic"), tu.getStringByField("reading_name"),
                        hasHost ? " of " + host : "", tu.getDoubleByField("value"),
                        tu.getDoubleByField("lower_threshold"), tu.getDoubleByField("upper_threshold"), 
                        max_duration);
			}
		}
	}

	private List<Double> check_timesince(List<Tuple> tuples) {
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
		 * howBad = -1 : no Alarm
         * howBad = 0 : alarm of level 0 
         * howBad = 1 : alarm of level 1 ... Define levels at some point (in Doberman)
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
            // check if values are iniside threshold of level j for tuples from newest to oldest 
			for (int i = tuples.size()-1; i >= 0; --i) { 
              Double pid = tuples.get(i).getDoubleByField("pid");
				if (pid < lower_threshold.get(j) || pid  > upper_threshold.get(j)) {
					recurrence += 1;
					if (recurrence >= max_recurrence) {
						howBad += 1.;
                        break;
					}
                }
                else {
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
}
