package bolts;

import org.apache.storm.shade.org.apache.commons.lang.NotImplementedException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

import java.util.Map;

/**
 * Created by jim on 23/5/2017.
 */
public class WeirdPressureBolt extends HealthBolt {
    protected int pressure_threshold = 0;
    protected int pressure_emergency_threshold = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);

        pressure_threshold = ((Long) map.get("PRESSURE_WRITE_THRESHOLD")).intValue();
        pressure_emergency_threshold = ((Long) map.get("PRESSURE_EMERGENCY_THRESHOLD")).intValue();
        this.emergencyFilter = new CheckThreshold() {
            @Override
            public <T extends Number> boolean checkThreshold(T t1, T t2) {
                Integer currentValue = (Integer) t1;
                return currentValue > pressure_emergency_threshold;
            }
        };
        this.thresholdCondition = new CheckThreshold() {
            @Override
            public <T extends Number> boolean checkThreshold(T t1, T t2) {
                Integer currentValue = (Integer) t1;
                Integer lastValue = (Integer) t2;
                return Math.abs(currentValue - lastValue)> pressure_threshold;
            }
        };
    }
    //
    // public  void emitEmergencyValues(String id, Number currentValue) throws NotImplementedException{
    //     if (emergencyFilter == null) throw new NullPointerException();
    //     Integer lastValueOfCurrentPatient = (Integer) values.get(id).get(values.get(id).size() - 1).getKey();
    //     if (emergencyFilter.checkThreshold(currentValue, 120)) {
    //         log.info("BLOOD PRESSURE OF PATIENT WITH ID " + id + " has exceeded normal levels");
    //         _collector.emit(EMERGENCY_STREAM, new Values(id, values.get(id)));
    //     }
    // }
    //
    // public  void compareDeltaThreshold(String id,Number currentValue, long timestamp) throws NotImplementedException{
    //     if (thresholdCondition == null) throw new NullPointerException();
    //     Integer lastValueOfCurrentPatient = (Integer) values.get(id).get(values.get(id).size() - 1).getKey();
    //     if (thresholdCondition.checkThreshold(lastValue, lastValueOfCurrentPatient)) {
    //         values.get(id).add(new Pair<>(currentValue, timestamp));
    //         insertDocument(id, currentValue, timestamp);
    //     }
    // }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(EMERGENCY_STREAM, new Fields("id", "pressure_array"));
        outputFieldsDeclarer.declareStream(REPLICA_REPORT_STREAM, new Fields("topic", "id", "pressure_array"));
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);
    }

    @Override
    public <T extends Number> void insertDocument(String id, T value, long timestamp) throws NotImplementedException {
        coll = db.getCollection("pressure_" + id);
        Document document = new Document("id", id).append("pressure", value).append("timestamp", timestamp);
        coll.insertOne(document);
    }
}
