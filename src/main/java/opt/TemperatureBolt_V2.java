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
public class TemperatureBolt_V2 extends HealthBolt_V2<Double> {
private  double temperature_emergency_threshold;
private  double temperature_delta_threshold;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        temperature_emergency_threshold = (double) map.get("TEMPERATURE_EMERGENCY_THRESHOLD");
        temperature_delta_threshold = (double) map.get("TEMPERATURE_WRITE_THRESHOLD");
    }

    @Override
    protected ThresholdInterface<Double> setConditionInterface() {
        return new ThresholdInterface<Double>() {
            @Override
            public boolean CompareDelta(Double currentValue, Double lastValue) {
                return Math.abs(currentValue - lastValue) > temperature_delta_threshold;
            }

            @Override
            public boolean EmergencyCondition(Double currentValue) {
                return currentValue>temperature_emergency_threshold;
            }
        };
    }


    @Override
    public void insertDocument(String id, Number value, long timestamp) {
       insertDocumentHelper(id,value,timestamp);
    }

    public <T extends Number> void insertDocumentHelper(String id, T temperature, long timestamp){
        coll = db.getCollection("temperature_" + id);
        Document document = new Document("id", id).append("temperature", temperature).append("timestamp", timestamp);
        coll.insertOne(document);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(EMERGENCY_STREAM, new Fields("id", "temperature_array"));
        outputFieldsDeclarer.declareStream(REPLICA_REPORT_STREAM, new Fields("topic", "id", "temperature_array"));
    }
}
