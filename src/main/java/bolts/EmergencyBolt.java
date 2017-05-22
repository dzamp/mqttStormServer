package bolts;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

import java.util.*;

import static com.mongodb.client.model.Filters.gt;

/**
 * Created by jim on 11/4/2017.
 */
public class EmergencyBolt implements IRichBolt {
    OutputCollector _collector;
    String id;
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected int reportFromTime;
    Block<Document> printBlock;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        dbClient = new MongoClient();
        db = dbClient.getDatabase("health_monitor");
        reportFromTime = Integer.valueOf((String)map.get("EMERGENCY_REPORT_TIME"));
        printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
    }

    @Override
    public void execute(Tuple tuple) {
        List<Object> values= tuple.getValues();
        ArrayList<Pair<? extends Number, Long>> patientValues = (ArrayList<Pair<? extends Number , Long>>) tuple.getValue(1);
        String id = tuple.getString(0);
        System.out.println("PATIENT WITH ID " + id + "has reached critical levels, assume action ");
        List<String> userCollections = returnUserCollections(id);
        // for (Pair<? extends Number,Long> pair : patientValues ){
        //     Number n = pair.getKey();
        //     long timestamp = pair.getValue();
        //     System.out.println("KEY : " + n + " VALUE " + timestamp);
        // }
        long queryFromTimestamp = patientValues.get(patientValues.size()-1).getValue() - 3_600_000 * reportFromTime;
        for(String col:userCollections){
            MongoCollection<Document> collectionDocuments = db.getCollection(col);
            FindIterable<Document> correctValues =  collectionDocuments.find(gt("timestamp",queryFromTimestamp)).sort(new Document("timestamp",1));
            correctValues.forEach(printBlock);
        }

    }

    private List<String>  returnUserCollections(final String id){
        List<String> userCollections = new ArrayList<>();
        MongoIterable<String> collections = db.listCollectionNames();
        for(String cName : collections) {
            if (cName.contains(id))
                userCollections.add(cName.trim());
        }
        return userCollections;
    }

    private long calculatePreviousTimestamp(){
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+3"));
        cal.add(Calendar.HOUR_OF_DAY , -reportFromTime);
        Date date = cal.getTime();
        return date.getTime();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("emergency"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
