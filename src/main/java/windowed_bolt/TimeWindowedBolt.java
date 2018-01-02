package windowed_bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimeWindowedBolt extends BaseWindowedBolt {
    OutputCollector collector ;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("value"));
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        List<Tuple> tuples= inputWindow.getNew();
        ArrayList<Object> tts = new ArrayList<>();
        for(Tuple t: tuples){
            tts.add((Object)t);
        }
        collector.emit(new Values(tts));
    }

}
