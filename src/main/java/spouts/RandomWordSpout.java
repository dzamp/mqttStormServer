package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.sql.Timestamp;
import java.util.Map;

public class RandomWordSpout extends BaseRichSpout {

    String[] words = new String[]{"The", "brown", "fox", "quick", "jump", "sucky", "5dolla"};
    SpoutOutputCollector collector;
    private String[] emmitedFields;

    public RandomWordSpout(String[] emmitedFields) {
        this.emmitedFields = emmitedFields;
    }

    public RandomWordSpout() {
        this.emmitedFields = new String[]{"word", "timestamp"};
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
        Utils.sleep(300);
        int rnd = (int) (Math.random() * 10 % words.length);
        collector.emit(new Values(words[rnd], new Timestamp(System.currentTimeMillis())));


    }
}