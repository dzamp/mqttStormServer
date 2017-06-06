package bolts;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.storm.shade.org.apache.commons.lang.NotImplementedException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jim on 23/5/2017.
 */
public class HealthBolt implements IRichBolt {
    public final static String REPLICA_REPORT_STREAM = "socket-replica-stream";
    public final static String EMERGENCY_STREAM = "emergency-stream";
    protected static int REPLICA_REPORT_THRESHOLD;
    protected OutputCollector _collector;
    protected Logger log;
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected MongoCollection<Document> coll;
    protected String MONGO_DB;
    protected HashMap<String, List<Pair<? extends Number, Long>>> values;
    protected int THRESHOLD_VALUE = 2;
    protected CheckThreshold thresholdCondition;
    protected CheckThreshold emergencyFilter;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.log = Logger.getLogger(this.getClass());
        MONGO_DB = (String) map.get("MONGO_DB");
        REPLICA_REPORT_THRESHOLD = ((Long) map.get("REPLICA_REPORT_THRESHOLD")).intValue();
        dbClient = new MongoClient();
        db = dbClient.getDatabase(MONGO_DB);
        values = new HashMap<>();
        //initialize threshold condition

    }



    @Override
    public void execute(Tuple tuple) {
        String id = /*tuple.getString(0);*/ tuple.getString(0);
        boolean newlyCreated = Boolean.FALSE;
        Number currentValue = (Number) tuple.getValue(1);
        long timestamp = tuple.getLong(2);
        if (!values.containsKey(id)) {
            newlyCreated = true;
            values.put(id, new ArrayList<>());
            values.get(id).add(new Pair<>(currentValue, timestamp));
            insertDocument(id, currentValue, timestamp);
        } else {

            compareDeltaThreshold(id, currentValue, timestamp);
        }
        emitEmergencyValues(id, currentValue);

        if (values.get(id).size() >= REPLICA_REPORT_THRESHOLD) { // 1000 * 1000 = 1MB
            log.info("MAX SIZE ");
            _collector.emit(REPLICA_REPORT_STREAM, new Values("pressure", id, values.get(id)));
            values.clear();
        }
    }

    public  void emitEmergencyValues(String id, Number currentValue) throws NotImplementedException{
        if (emergencyFilter == null) throw new NullPointerException();
        if (emergencyFilter.checkThreshold(currentValue, 0)) {
            log.info("BLOOD PRESSURE OF PATIENT WITH ID " + id + " has exceeded normal levels");
            _collector.emit(EMERGENCY_STREAM, new Values(id, values.get(id)));
        }
    }

    public  void compareDeltaThreshold(String id,Number currentValue, long timestamp) throws NotImplementedException{
        if (thresholdCondition == null) throw new NullPointerException();
        Number lastValueOfCurrentPatient = /*(Integer) */values.get(id).get(values.get(id).size() - 1).getKey();
        if (thresholdCondition.checkThreshold(currentValue, lastValueOfCurrentPatient)) {
            values.get(id).add(new Pair<>(currentValue, timestamp));
            insertDocument(id, currentValue, timestamp);
        }
    }



    public <T extends Number> void insertDocument(String id, T value, long timestamp) throws NotImplementedException {

    }

    @Override
    public void cleanup() {

    }

    // public void insertDocument(String id, int pressureValue, long timestamp) {
    //     coll = db.getCollection("pressure_" + id);
    //     Document document = new Document("id", id).append("pressure", pressureValue).append("timestamp", timestamp);
    //     coll.insertOne(document);
    // }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) throws NotImplementedException {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    protected interface CheckThreshold {
         <T extends Number> boolean checkThreshold(T t1, T t2) ;
    }
}
