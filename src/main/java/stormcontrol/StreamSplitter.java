package stormcontrol;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class StreamSplitter extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final List<String> topics = Arrays.asList("pressure", "voltage", "temperature",
            "current", "status", "power", "level", "sysmon", "other");
	private OutputCollector collector;
    private int numCounterTasks;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
        this.numCounterTasks = context.getComponentTasks("Buffer").size();
	}

	@Override
	public void execute(Tuple input) {
		String topic = input.getStringByField("topic");
		int task_id = topics.indexOf(topic) + 1;
		collector.emitDirect(task_id, "direct_stream",
				new Values(topic, input.getDoubleByField("timestamp"), input.getStringByField("host"),
						input.getStringByField("reading_name"), input.getStringByField("value")));
        collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("direct_stream", true, new Fields("topic", "timestamp", "host", "reading_name", "value"));

	}

}
