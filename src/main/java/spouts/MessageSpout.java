package spouts;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import javafx.util.Pair;
import org.apache.storm.shade.org.eclipse.jetty.io.RuntimeIOException;
import org.apache.storm.shade.org.eclipse.jetty.util.BlockingArrayQueue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.eclipse.paho.client.mqttv3.*;

import java.util.Map;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jim on 7/4/2017.
 */
public class MessageSpout implements IRichSpout, MqttCallback {
    //    Logger log = Logger.getLogger(MessageSpout.class);
    public final static String PRESSURE_STREAM = "pressureStream";
    public final static String PRESSURE_TOPIC = "health_monitor/blood_pressure";
    public final static String OXYGEN_STREAM = "oxygenStream";
    public final static String OXYGEN_TOPIC = "health_monitor/oxygen_saturation";
    public final static String CLIENT_SUBSCRIBE = "health_monitor/subscribe_client";
    private  String MONGO_DB;
    MongoClient dbClient;
    protected MongoDatabase db;
    SpoutOutputCollector collector;
    MqttClient client;
    BlockingQueue<Pair<String,MqttMessage>> messageQueue; //A key value store of a topic, MqttMessage


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.out.println("MessageSpout.open");
        MONGO_DB = (String) map.get("MONGO_DB");
        this.collector = spoutOutputCollector;
        try {
//            client = new MqttClient("tcp://workstation00.raspberryip.com:1883", "Sending"+ Time.currentTimeMillis()); //persistence error?
            client = new MqttClient("tcp://localhost:1883", "Sending"+ Time.currentTimeMillis());
            client.connect();
            client.setCallback(this);
            client.subscribe("health_monitor/#");
            messageQueue = new BlockingArrayQueue<>();
        } catch (MqttException e) {
            e.printStackTrace();
        }
        dbClient = new MongoClient();
        db = dbClient.getDatabase(MONGO_DB);

    }

    @Override
    public void close() {
        System.out.println("MessageSpout.close");
    }

    @Override
    public void activate() { /* 1rst call */
        System.out.println("MessageSpout.activate");
    }

    @Override
    public void deactivate() {
        System.out.println("MessageSpout.deactivate");
    }

    @Override
    public void nextTuple() {
        while (!messageQueue.isEmpty()) {
            MqttMessage message = null;
            String topic = null;
            try {
                Pair<String,MqttMessage> p = messageQueue.take();
                message =  p.getValue();
                topic = p.getKey();
                if (message != null && topic!=null) {
                    String[] values = message.toString().split(",");

                    switch (topic) {
                        case PRESSURE_TOPIC:
                            System.out.println("PRESSURE : Printing values of topic " + topic + " " + values[0] + " " + values[1]);
                            collector.emit(PRESSURE_STREAM, new Values(values[0], values[1]));
                            break;
                        case OXYGEN_TOPIC:
                            System.out.println("OXYGEN : Printing values of topic " + topic + " " + values[0] + " " + values[1]);
                            collector.emit(OXYGEN_STREAM, new Values(values[0], values[1]));
                            break;
                        case CLIENT_SUBSCRIBE:
                            // db.createCollection("pressure"+"_"+values[0], new CreateCollectionOptions().capped(true).sizeInBytes(0x1000000));
                            // db.createCollection("oxygen"+"_"+values[0], new CreateCollectionOptions().capped(true).sizeInBytes(0x1000000));
                            // db.createCollection("temperature"+"_"+values[0], new CreateCollectionOptions().capped(true).sizeInBytes(0x1000000));
                            break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    @Override
    public void ack(Object o) {
        System.out.println("MessageSpout.ack");
    }

    @Override
    public void fail(Object o) {
        System.out.println("MessageSpout.fail");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //1 named streams example
        outputFieldsDeclarer.declareStream(PRESSURE_STREAM, new Fields("id", "pressure_value"));
        outputFieldsDeclarer.declareStream(OXYGEN_STREAM, new Fields("id", "oxygen_value"));
        //2 old way,
        //outputFieldsDeclarer.declare(new Fields("id", "pressure_value"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    /* MqttCallback Implementation */
    @Override
    public void connectionLost(Throwable throwable) {
        throw new RuntimeIOException();
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        messageQueue.put(new Pair<>(topic.trim(), mqttMessage));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("MessageSpout.deliveryComplete");
    }
}
