package fusion_demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jim on 1/9/2017.
 */
public class WordCounter  extends BaseRichBolt {
    private HashMap<String,Integer> countMap;

    OutputCollector collector;
    String id;

    public WordCounter() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        countMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        if(!countMap.containsKey(tuple.getString(0))){
            countMap.put(tuple.getString(0),0);
        }else {
            countMap.put(tuple.getString(0), countMap.get(tuple.getString(0)+1));
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //nothing
    }
}
