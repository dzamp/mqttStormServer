package bolts;

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
public class TemperatureBolt extends HealthBolt<Double> {

    protected double temperature_threshold = 0.0;
    protected double temperature_emergency_threshold = 0;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
        temperature_threshold = ((Double) map.get("TEMPERATURE_WRITE_THRESHOLD")).doubleValue();
        temperature_emergency_threshold = ((Double) map.get("TEMPERATURE_EMERGENCY_THRESHOLD")).doubleValue();
        super.prepare(map, topologyContext, outputCollector);
    }

    @Override
    protected HashMap<String, List<Pair<Double, Long>>> setValues() {
        return new HashMap<>();
    }

    @Override
    public void emitEmergencyValues(String id, Double currentValue) {
        if(currentValue > temperature_emergency_threshold){
            log.info("TEMPERATURE OF PATIENT WITH ID " + id + " has exceeded normal levels");
            _collector.emit(EMERGENCY_STREAM, new Values( id, values.get(id),  currentValue, "temperature"));
        }
    }

    @Override
    public void compareDeltaThreshold(String id, Double currentValue, long timestamp) {
        double lastValueOfCurrentPatient = values.get(id).get(values.get(id).size() - 1).getKey();
        if (Math.abs(currentValue - lastValueOfCurrentPatient) >= temperature_threshold) {
            values.get(id).add(new Pair<>(currentValue, timestamp));
            insertDocument(id, currentValue, timestamp);
        }
    }

    @Override
    public void insertDocument(String id, Double currentValue, long timestamp) {
        coll = db.getCollection("temperature_" + id);
        Document document = new Document("id", id).append("temperature", currentValue).append("timestamp", timestamp);
        coll.insertOne(document);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(EMERGENCY_STREAM, new Fields("id", "temperature_array","emergency_value", "topic"));
        outputFieldsDeclarer.declareStream(REPLICA_REPORT_STREAM, new Fields("topic", "id", "temperature_array"));
    }
}
