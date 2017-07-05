package bolts;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;


/**
 * Created by jim on 11/4/2017.
 */
public class EmergencyBolt implements IRichBolt {
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected int reportFromTime;
    OutputCollector _collector;
    String id;
    Block<Document> printBlock;
    SocketChannel client;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        dbClient = new MongoClient();
        db = dbClient.getDatabase("health_monitor");
        reportFromTime = Integer.valueOf((String) map.get("EMERGENCY_REPORT_TIME"));
        printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        InetSocketAddress hostAddress = new InetSocketAddress("localhost", 8100);
        try {
            client = SocketChannel.open(hostAddress);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        List<Object> values = tuple.getValues();
        ArrayList<Pair<? extends Number, Long>> patientValues = (ArrayList<Pair<? extends Number, Long>>) tuple.getValue(1);
        String id = tuple.getString(0);
        System.out.println("PATIENT WITH ID " + id + " has reached critical levels, assume action ");
        List<String> userCollections = returnUserCollections(id);

        // long queryFromTimestamp = patientValues.get(patientValues.size() - 1).getValue() - 3_600_000 * reportFromTime;
        // long queryFromTimestamp = patientValues.get(patientValues.size() - 1).getValue() - 60 * 60 * reportFromTime;
        // for (String col : userCollections) {
        //     MongoCollection<Document> collectionDocuments = db.getCollection(col);
        //     FindIterable<Document> correctValues = collectionDocuments.find(gt("timestamp", queryFromTimestamp)).sort(new Document("timestamp", 1));
        //     correctValues.forEach(printBlock);
        //     String output = "";
        //     Iterator<Document> iterator = correctValues.iterator();
        //     while (iterator.hasNext()) {
        //         String topics[] = col.split("_");
        //         String topic = topics[0].trim();
        //         Document doc = iterator.next();
        //         output = output.concat(topic.trim() + "%" + tuple.getValue(0).toString() + "%" + doc.get(topic.trim()) + "%" + doc.get("timestamp") + "\n");
        //     }
        //     ByteBuffer buffer = null;
        //     buffer = ByteBuffer.wrap(output.getBytes());
        //     try {
        //         client.write(buffer);
        //     } catch (IOException e) {
        //         e.printStackTrace();
        //     }
        // }

        sendEmergencyValueOnlytoGui(patientValues, userCollections, tuple);


    }

    private void sendEmergencyValueOnlytoGui(ArrayList<Pair<? extends Number, Long>> patientValues, List<String> userCollections, Tuple tuple) {
        long queryFromTimestamp = patientValues.get(patientValues.size() - 1).getValue() - 60 * reportFromTime;
        Number n = null;
        String output = "";

        n = (Number) tuple.getValueByField("emergency_value");
        for (int i =0; i< patientValues.size(); i++) {
            String pressure_value = String.valueOf(patientValues.get(i).getKey());
            String num = String.valueOf(n);
            if (num.equals(pressure_value)) {
                output = output.concat(tuple.getValueByField("topic") + "%" + tuple.getValue(0).toString() + "%" + num + "%" + patientValues.get(i).getValue() + "\n");
            }
        }

        ByteBuffer buffer = null;
        buffer = ByteBuffer.wrap(output.getBytes());
        try {
            client.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // for (String col : userCollections) {
        //     MongoCollection<Document> collectionDocuments = db.getCollection(col);
        //     FindIterable<Document> correctValues = collectionDocuments.find(gt("timestamp", queryFromTimestamp)).sort(descending("pressure"));
        //     Iterator<Document> iterator = correctValues.iterator();
        //     String output = "";
        //     if (iterator.hasNext()) {
        //         String topics[] = col.split("_");
        //         String topic = topics[0].trim();
        //         Document doc = iterator.next();
        //         output = output.concat(topic.trim() + "%" + tuple.getValue(0).toString() + "%" + doc.get(topic.trim()) + "%" + doc.get("timestamp") + "\n");
        //     }
        //     ByteBuffer buffer = null;
        //     buffer = ByteBuffer.wrap(output.getBytes());
        //     try {
        //         client.write(buffer);
        //     } catch (IOException e) {
        //         e.printStackTrace();
        //     }
        // }
    }

    private List<String> returnUserCollections(final String id) {
        List<String> userCollections = new ArrayList<>();
        MongoIterable<String> collections = db.listCollectionNames();
        for (String cName : collections) {
            if (cName.contains(id))
                userCollections.add(cName.trim());
        }
        return userCollections;
    }

    private long calculatePreviousTimestamp() {
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+3"));
        cal.add(Calendar.HOUR_OF_DAY, -reportFromTime);
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
