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
 * Created by jim on 5/5/2017.
 */
public class TemperatureBolt implements IRichBolt{
    protected OutputCollector _collector;
    protected HashMap<String, List<Pair<Double, Long>>> temperatureValues;
    protected Logger log;
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected MongoCollection<Document> coll;
    protected HashMap<String, MongoCollection<Document>> mongoClientCollections;
    protected String MONGO_DB;
    protected double temperature_emergency_threshold = 0;
    protected double temperature_write_threshold = 0;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.log= Logger.getLogger(this.getClass());
        mongoClientCollections = new HashMap<>();
        temperatureValues = new HashMap<>();
        temperature_emergency_threshold = (double) map.get("TEMPERATURE_EMERGENCY_THRESHOLD");
        temperature_write_threshold = (double) map.get("TEMPERATURE_WRITE_THRESHOLD");
        MONGO_DB = (String) map.get("MONGO_DB");
        dbClient = new MongoClient();
        db = dbClient.getDatabase(MONGO_DB);
    }

    @Override
    public void execute(Tuple tuple) {
        String id = /*tuple.getString(0);*/ tuple.getString(0);
        boolean newlyCreated = false;
        if (!temperatureValues.containsKey(id)) {
            newlyCreated = true;
            temperatureValues.put(id, new ArrayList<>());
        }
        long timestamp = tuple.getLong(2);
        double currentTemperatureValue = tuple.getDouble(1);
        if (newlyCreated) {
            temperatureValues.get(id).add(new Pair<>(currentTemperatureValue, timestamp));
            coll = db.getCollection("temperature_" + id);
            Document document = new Document("id", id).append("temperature", currentTemperatureValue).append("timestamp", timestamp);
            coll.insertOne(document);
        }
        else {
            double lastTemperatureValueOfCurrentPatient =  temperatureValues.get(id).get(temperatureValues.get(id).size()-1).getKey();
            if (Math.abs(lastTemperatureValueOfCurrentPatient - currentTemperatureValue) > temperature_write_threshold) {
                temperatureValues.get(id).add(new Pair<>(currentTemperatureValue, timestamp));
                coll = db.getCollection("temperature_" + id);
                Document document = new Document("id", id).append("temperature", currentTemperatureValue).append("timestamp", timestamp);
                coll.insertOne(document);
            }
        }
        if (currentTemperatureValue >= temperature_emergency_threshold) {
            log.info("TEMPERATURE OF PATIENT WITH ID " + id + " has exceeded normal levels");
            _collector.emit(new Values(id, temperatureValues.get(id)));
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "temperature_array"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

