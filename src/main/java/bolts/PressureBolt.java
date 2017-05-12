package bolts;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.*;

/**
 * Created by jim on 4/5/2017.
 */
public class PressureBolt implements IRichBolt {
    protected OutputCollector _collector;
    protected HashMap<String, List<Pair<Integer, Long>>> pressureValues;
    protected Logger log;
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected MongoCollection<Document> coll;
    protected HashMap<String, MongoCollection<Document>> mongoClientCollections;
    protected String MONGO_DB;
    protected int pressure_threshold = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        mongoClientCollections = new HashMap<>();
        pressureValues = new HashMap<>();
        pressure_threshold = Integer.valueOf((String) map.get("PRESSURE_WRITE_THRESHOLD"));
        MONGO_DB = (String) map.get("MONGO_DB");
        dbClient = new MongoClient();
        db = dbClient.getDatabase(MONGO_DB);
        // coll =  db.getCollection("pressure");

    }


    //We have to think that each bolt deals with many messages regardless if they have the same id.
    //I need to define an algorithmic way of knowing where to put the collection. A nice idea would be to use <whatAreWeMonitoring_clientID>
    //so for patiend with id ae21efj23 we will use the collection pressure_ae21efj23

    //We will write stuff whenever the max size of the map peaks to 1/2 M or 100KB we will see,
    //or whenever the thread memory usage goes beyond a threshold


    @Override
    public void execute(Tuple tuple) {
        long timestamp = new Date().getTime();
        String id = /*tuple.getString(0);*/ tuple.getString(0);
        boolean newlyCreated = Boolean.FALSE;
        if (!pressureValues.containsKey(id)) {
            newlyCreated = Boolean.TRUE;
            pressureValues.put(id, new ArrayList<>());
        }

        int currentPressureValue = Integer.valueOf(tuple.getString(1));
        if (newlyCreated) {
            pressureValues.get(id).add(new Pair<>(currentPressureValue, timestamp));
            coll = db.getCollection("pressure_" + id);
            Document document = new Document("id", id).append("pressure", currentPressureValue).append("timestamp", timestamp);
            coll.insertOne(document);
        }
        else {
            int lastPressureValueOfCurrentPatient = pressureValues.get(id).get(pressureValues.get(id).size()).getKey();
            if (Math.abs(lastPressureValueOfCurrentPatient - currentPressureValue) > 2) {
                pressureValues.get(id).add(new Pair<>(currentPressureValue, timestamp));
                coll = db.getCollection("pressure_" + id);
                Document document = new Document("id", id).append("pressure", currentPressureValue).append("timestamp", timestamp);
                coll.insertOne(document);
            }
        }
        if (currentPressureValue >= 120) {
            log.info("BLOOD PRESSURE OF PATIENT WITH ID " + id + " has exceeded normal levels");
            _collector.emit(new Values(id, pressureValues.get(id)));
        }
    }

    @Override
    public void cleanup() {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "pressure_array"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
