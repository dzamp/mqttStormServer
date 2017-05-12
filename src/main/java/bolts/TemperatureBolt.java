package bolts;

import org.apache.log4j.Logger;
import org.apache.storm.shade.org.apache.commons.lang.NotImplementedException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by jim on 5/5/2017.
 */
public class TemperatureBolt implements IRichBolt{
   private HashMap<String,List<Double>> temperatureValues;
    protected OutputCollector _collector;
    protected Logger log;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        temperatureValues = new HashMap<>();
        log = Logger.getLogger(this.getClass());
    }

    @Override
    public void execute(Tuple tuple) {
        String id = tuple.getString(0);
        if(!temperatureValues.containsKey(id)) {
            temperatureValues.put(id, new ArrayList<>());
        }
        double temperature = tuple.getDouble(0);
        temperatureValues.get(id).add(temperature);
        if(temperature>=38){
            log.info("TEMPERATURE OF PATIENT WITH ID " + id + " has exceeded normal levels");
            _collector.emit(new Values(id, temperatureValues.get(id)));
        }
    }

    @Override
    public void cleanup() {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
