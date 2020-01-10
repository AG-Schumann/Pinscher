package Greyhound;

import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

public class LogAlarm extends BaseRichBolt{

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
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

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
