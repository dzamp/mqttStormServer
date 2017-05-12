package bolts;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.apache.storm.shade.org.apache.commons.lang.NotImplementedException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;


/**
 * Created by jim on 4/5/2017.
 */
public class OxygenSaturationBolt implements IRichBolt{
    protected OutputCollector _collector;
    protected HashMap<String,List<Integer >> saturationValues;
    protected Logger log;
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected MongoCollection<Document> coll;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        saturationValues = new HashMap<>();
        log = Logger.getLogger(this.getClass());

        dbClient = new MongoClient();
        db  = dbClient.getDatabase("health_monitor");
        coll =  db.getCollection("pressure");
        coll.find().forEach(new Consumer<Document>() {
            @Override
            public void accept(Document document) {
                System.out.println("+++++++++++++++++++++++++++++++" + document.toJson().toString());
            }
        });
    }



    @Override
    public void execute(Tuple tuple) {
        String id = /*tuple.getString(0);*/ tuple.getString(0);

        if(!saturationValues.containsKey(id)) {
            saturationValues.put(id, new ArrayList<>());
        }
        int value = Integer.valueOf(tuple.getString(1));
        saturationValues.get(id).add(value);
        if(value <= 96){
            log.info("OXYGEN SATURATION OF PATIENT WITH ID " + id + " has exceeded normal levels");
            _collector.emit(new Values(id, saturationValues.get(id)));
        }
    }

    @Override
    public void cleanup() {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "oxygen_saturation_array"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
