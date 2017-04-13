package health.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jim on 10/4/2017.
 */
public class PressureAlert implements IRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("PressureAlert.execute");
       HealthMonitorMessage message= (HealthMonitorMessage)tuple.getValue(0);
//        ObjectMapper mapper =  new ObjectMapper();
//        HealthMonitorMessage message = null;
//        try {
//            message = mapper.readValue(jsonMessage, HealthMonitorMessage.class);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        _collector.emit(new Values(message.toString()));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
