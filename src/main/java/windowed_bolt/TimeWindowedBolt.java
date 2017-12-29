package windowed_bolt;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class TimeWindowedBolt extends BaseWindowedBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("value"));
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        List<Tuple> tuples= inputWindow.get();
        ArrayList<Tuple> tts = new ArrayList<>();
        tuples.addAll(tts);

    }

}
