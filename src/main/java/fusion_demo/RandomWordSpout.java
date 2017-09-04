package fusion_demo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by jim on 1/9/2017.
 */
public class RandomWordSpout extends BaseRichSpout {

    String[] words = new String[]{"The", "brown", "fox", "quick", "jump", "sucky", "5dolla"};
    SpoutOutputCollector collector;
    private String[] emmitedFields;

    public RandomWordSpout(String[] emmitedFields) {
        this.emmitedFields = emmitedFields;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(emmitedFields));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        while (true) {
            int rnd = (int) (Math.random() * 10 % words.length);
            collector.emit(new Values(words[rnd]));
            try {
                Thread.currentThread().sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}