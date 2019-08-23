package doberman.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class DebugBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String message = "DEBUG TUPLE: " + input.getMessageId().toString();
        System.out.println("DEBUGTUPLE: " + input);
        /*try {
            Thread.sleep(10000);
        } catch (Exception InterruptedException) {
            System.out.println("DebugBolt: Sleep interupted");

        }*/


        //System.out.println("Topic: " + input.getString(0) + "    Timestamp: " +
        //              input.getDouble(1) + "    Value: " + input.getDouble(4));
        collector.emit(new Values(message));
        collector.ack(input);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}

