package opt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.bson.Document;

import java.util.Map;

/**
 * Created by jim on 29/5/2017.
 */
public class OxygenSaturationBolt_V2 extends HealthBolt_V2<Integer> {
    private int oxygen_emergency_threshold;
    private int oxygen_delta_threshold;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        oxygen_emergency_threshold = ((Long) map.get("OXYGEN_EMERGENCY_THRESHOLD")).intValue();
        oxygen_delta_threshold = ((Long) map.get("OXYGEN_WRITE_THRESHOLD")).intValue();
    }

    @Override
    protected ThresholdInterface<Integer> setConditionInterface() {
        return new ThresholdInterface<Integer>() {
            @Override
            public boolean CompareDelta(Integer currentValue, Integer lastValue) {
                return Math.abs(currentValue - lastValue) >= oxygen_delta_threshold;
            }

            @Override
            public boolean EmergencyCondition(Integer currentValue) {
                return oxygen_emergency_threshold > currentValue ;
            }
        };
    }



    @Override
    public void insertDocument(String id, Number value, long timestamp) {
        insertDocumentHelper(id,value,timestamp);
    }


    public <T extends Number> void insertDocumentHelper(String id, T oxygen, long timestamp){
        coll = db.getCollection("oxygen_" + id);
        Document document = new Document("id", id).append("oxygen", oxygen).append("timestamp", timestamp);
        coll.insertOne(document);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(EMERGENCY_STREAM, new Fields("id", "oxygen_array"));
        outputFieldsDeclarer.declareStream(REPLICA_REPORT_STREAM, new Fields("topic", "id", "oxygen_array"));
    }
}
