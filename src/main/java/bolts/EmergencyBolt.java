package bolts;

import org.apache.storm.shade.org.eclipse.jetty.io.RuntimeIOException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jim on 11/4/2017.
 */
public class EmergencyBolt implements IRichBolt {
    OutputCollector _collector;
    String id;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        List<?> values = (ArrayList<?>)tuple.getValue(1);
        String id = tuple.getString(0);
//        if (this.id != id)throw new RuntimeIOException("WHAT IS THIS ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
//        System.out.println("Printing values of pressure prior emergency for partient with id " + id);
//        values.forEach(s -> System.out.println(s));
//        _collector.emit(new Values(values));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("emergency"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
