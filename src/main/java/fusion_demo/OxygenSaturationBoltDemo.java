package fusion_demo;

import bolts.HealthBolt;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jim on 30/6/2017.
 */
public class OxygenSaturationBoltDemo extends HealthBoltDemo<Integer> {
    private int oxygen_emergency_threshold;
    private int oxygen_delta_threshold;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
        super.prepare(map, topologyContext, outputCollector);
        oxygen_emergency_threshold = ((Long) map.get("OXYGEN_EMERGENCY_THRESHOLD")).intValue();
        oxygen_delta_threshold = ((Long) map.get("OXYGEN_WRITE_THRESHOLD")).intValue();
    }

    @Override
    protected HashMap<String, List<Pair<Integer, Long>>> setValues() {
        return new HashMap<>();
    }

    @Override
    public void emitEmergencyValues(String id, Integer currentValue) {
        if(currentValue < oxygen_emergency_threshold){
            log.info("OXYGEN SATURATION OF PATIENT WITH ID " + id + " has exceeded normal levels");
            _collector.emit(EMERGENCY_STREAM, new Values(id, values.get(id),currentValue, "oxygen"));
        }
    }

    @Override
    public void compareDeltaThreshold(String id, Integer currentValue, long timestamp) {
        int lastValueOfCurrentPatient = values.get(id).get(values.get(id).size() - 1).getKey();
        if (Math.abs(currentValue - lastValueOfCurrentPatient) >= oxygen_delta_threshold ) {
            values.get(id).add(new Pair<>(currentValue, timestamp));
            // insertDocument(id, currentValue, timestamp);
        }
    }

    @Override
    public void insertDocument(String id, Integer currentValue, long timestamp) {
        coll = db.getCollection("oxygen_" + id);
        Document document = new Document("id", id).append("oxygen", currentValue).append("timestamp", timestamp);
        coll.insertOne(document);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(EMERGENCY_STREAM, new Fields("id", "oxygen_array","emergency_value", "topic"));
        // outputFieldsDeclarer.declareStream(REPLICA_REPORT_STREAM, new Fields("topic", "id", "oxygen_array"));
    }
}
