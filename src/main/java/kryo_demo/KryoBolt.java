package kryo_demo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class KryoBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        FusionValues values = (FusionValues) input.getValueByField("FusionValues");
        System.out.println(values.value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
