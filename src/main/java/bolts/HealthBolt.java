package bolts;

import org.apache.log4j.Logger;
import org.apache.storm.shade.org.apache.commons.lang.NotImplementedException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class HealthBolt implements IRichBolt {
    protected OutputCollector _collector;
    protected HashMap<Integer,List<? super Number >> values;
    protected Logger log;
    protected int id;
    protected int currentValue;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        values = new HashMap<>();
        log = Logger.getLogger(this.getClass());

    }

    @Override
    public void execute(Tuple tuple) {
        id = /*tuple.getString(0);*/ Integer.valueOf(tuple.getString(0));

        if(!values.containsKey(id)) {
            values.put(id, new ArrayList<>());
        }
        currentValue = Integer.valueOf(tuple.getString(1));
        values.get(id).add(currentValue);

        // LOGIC IMPLEMENTED HERE
        // watchCriticalLevels(values,currentValue, id);
        // well see if we should emit values here

    }

    public    void watchCriticalLevels(HashMap<Integer,List<Integer>> collectedValues, int currentValue, int currentPatientId ) throws NotImplementedException{ }

    public   void watchCriticalLevels(HashMap<Integer,List<Double>> collectedValues, double currentValue, int currentPatientId ) throws NotImplementedException{ }

    @Override
    public void cleanup() {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) throws NotImplementedException{

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
