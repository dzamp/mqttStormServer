package timestamp_join;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;

public class UUIDSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {

        while(true){
            collector.emit(new Values(UUID.randomUUID().toString(),new Timestamp(System.currentTimeMillis()%1000).getTime()));
            Utils.sleep(300);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uuid", "timestamp"));
    }
}
