package fusion_demo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class HealthBoltDemo<T extends Number> implements IRichBolt {
    public final static String REPLICA_REPORT_STREAM = "socket-replica-stream";
    public final static String EMERGENCY_STREAM = "emergency-stream";
    protected static int REPLICA_REPORT_THRESHOLD;
    protected OutputCollector _collector;
    protected Logger log;
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected MongoCollection<Document> coll;
    protected String MONGO_DB;
    protected HashMap<String, List<Pair<T, Long>>> values;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.log = Logger.getLogger(this.getClass());
        MONGO_DB = (String) map.get("MONGO_DB");
        REPLICA_REPORT_THRESHOLD = ((Long) map.get("REPLICA_REPORT_THRESHOLD")).intValue();
        dbClient = new MongoClient();
        db = dbClient.getDatabase(MONGO_DB);
        // values = new HashMap<>();
        //initialize threshold condition
        // setConditionalInterface();
        // this.conditionInterface = setConditionInterface();
        this.values = setValues();
    }

    protected abstract HashMap<String, List<Pair<T, Long>>> setValues();


    @Override
    public void execute(Tuple tuple) {
        String id = /*tuple.getString(0);*/ tuple.getString(0);
        boolean newlyCreated = false;
        T currentValue = (T) tuple.getValue(1);
        long timestamp = tuple.getLong(2);
        if (!values.containsKey(id)) {
            newlyCreated = true;
            values.put(id, new ArrayList<>());
            values.get(id).add(new Pair<>(currentValue, timestamp));
            // insertDocument(id, currentValue, timestamp);
        } else {
            compareDeltaThreshold(id, currentValue, timestamp);
        }
        emitEmergencyValues(id, currentValue);

        if (values.get(id).size() >= REPLICA_REPORT_THRESHOLD) { // 1000 * 1000 = 1MB
            log.info("MAX SIZE ");
            // _collector.emit(REPLICA_REPORT_STREAM, new Values("pressure", id, values.get(id)));
            values.clear();
        }
    }


    public abstract void emitEmergencyValues(String id, T currentValue) ;

    public abstract void compareDeltaThreshold(String id, T currentValue, long timestamp) ;

    @Override
    public void cleanup() {
        this.dbClient.close();
    }



    public abstract void insertDocument(String id, T currentValue, long timestamp) ;

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
