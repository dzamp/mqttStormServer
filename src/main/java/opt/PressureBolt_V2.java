package opt;

import com.mongodb.client.MongoCollection;
import org.apache.storm.shade.org.apache.commons.lang.NotImplementedException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

import java.util.Map;

/**
 * Created by jim on 29/5/2017.
 */
public class PressureBolt_V2 extends HealthBolt_V2<Integer> {
    protected int pressure_threshold = 0;
    protected int pressure_emergency_threshold = 0;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        pressure_threshold = ((Long) map.get("PRESSURE_WRITE_THRESHOLD")).intValue();
        pressure_emergency_threshold = ((Long) map.get("PRESSURE_EMERGENCY_THRESHOLD")).intValue();
        super.prepare(map, topologyContext, outputCollector);
    }

    @Override
    protected ThresholdInterface<Integer> setConditionInterface() {
        return new ThresholdInterface<Integer>() {
            @Override
            public boolean CompareDelta(Integer currentValue, Integer lastValue) {
                return Math.abs(currentValue - lastValue) > pressure_threshold;
            }

            @Override
            public boolean EmergencyCondition(Integer currentValue) {
                return currentValue > pressure_emergency_threshold;
            }
        };
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(EMERGENCY_STREAM, new Fields("id", "pressure_array"));
        outputFieldsDeclarer.declareStream(REPLICA_REPORT_STREAM, new Fields("topic", "id", "pressure_array"));
    }



    public <T extends Number> void insertDocumentHelper(String id, T  pressureValue, long timestamp) {
        MongoCollection<Document> coll = db.getCollection("pressure_" + id);
                Document document = new Document("id", id).append("pressure", pressureValue).append("timestamp", timestamp);
                coll.insertOne(document);
    }

    @Override
    public void insertDocument(String id, Number pressureValue, long timestamp) {
        insertDocumentHelper(id,pressureValue,timestamp);
    }


}
