package Pinscher;

import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

public class LogAlarm extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private ConfigDB config_db;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
        	String experiment_name = (String) topoConf.get("EXPERIMENT_NAME");
        	String mongo_uri = (String) topoConf.get("MONGO_CONNECTION_URI");
        	config_db = new ConfigDB(mongo_uri, experiment_name);
	}

	@Override
	public void execute(Tuple input) {
		String msg = input.getStringByField("msg");
		Double howBad = input.getDoubleByField("howBad");
		Document log = new Document();
		log.put("when", new Date(input.getDoubleByField("timestamp").longValue()));
		log.put("howbad", howBad);
		log.put("msg", msg);
		config_db.writeOne("logging", "alarm_history", log);
		collector.emit(new Values("logged"));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("logged"));

	}

}
