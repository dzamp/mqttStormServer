package bolts;

import com.mongodb.client.MongoCollection;
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
 * Created by jim on 6/6/2017.
 */
public class PressureBolt extends HealthBolt<Integer> {

    protected int pressure_threshold = 0;
    protected int pressure_emergency_threshold = 0;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        pressure_threshold = ((Long) map.get("PRESSURE_WRITE_THRESHOLD")).intValue();
        pressure_emergency_threshold = ((Long) map.get("PRESSURE_EMERGENCY_THRESHOLD")).intValue();
        super.prepare(map, topologyContext, outputCollector);
    }

    @Override
    protected HashMap<String, List<Pair<Integer, Long>>> setValues() {
        return new HashMap<>();
    }

    @Override
    public void emitEmergencyValues(String id, Integer currentValue) {
        if(currentValue > pressure_emergency_threshold){
            log.info("BLOOD PRESSURE OF PATIENT WITH ID " + id + " has exceeded normal levels");
            _collector.emit(EMERGENCY_STREAM, new Values(id, values.get(id)));
        }
    }

    @Override
    public void compareDeltaThreshold(String id, Integer currentValue, long timestamp) {
        int lastValueOfCurrentPatient = values.get(id).get(values.get(id).size() - 1).getKey();
        if (Math.abs(currentValue - lastValueOfCurrentPatient) >= pressure_emergency_threshold ) {
            values.get(id).add(new Pair<>(currentValue, timestamp));
            insertDocument(id, currentValue, timestamp);
        }
    }

    @Override
    public void insertDocument(String id, Integer currentValue, long timestamp) {
        MongoCollection<Document> coll = db.getCollection("pressure_" + id);
        Document document = new Document("id", id).append("pressure", currentValue).append("timestamp", timestamp);
        coll.insertOne(document);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(EMERGENCY_STREAM, new Fields("id", "pressure_array"));
        outputFieldsDeclarer.declareStream(REPLICA_REPORT_STREAM, new Fields("topic", "id", "pressure_array"));
    }
}
