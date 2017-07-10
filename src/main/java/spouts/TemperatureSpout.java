package spouts;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
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
import java.util.concurrent.BlockingQueue;

/**
 * Created by jim on 7/4/2017.
 */
public class TemperatureSpout implements IRichSpout, MqttCallback {
    public final static String TEMPERATURE_TOPIC = "health_monitor/temperature";
    public final static String TEMPERATURE_STREAM = "temperatureStream";
    private  String MONGO_DB;
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected SpoutOutputCollector collector;
    protected MqttClient client;
    protected BlockingQueue<Pair<String,MqttMessage>> messageQueue; //A key value store of a topic, MqttMessage


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.out.println("PressureSpout.open");
        MONGO_DB = (String) map.get("MONGO_DB");
        this.collector = spoutOutputCollector;
        try {
//            client = new MqttClient("tcp://workstation00.raspberryip.com:1883", "Sending"+ Time.currentTimeMillis()); //persistence error?
            client = new MqttClient("tcp://localhost:1883", "Sending"+ Time.currentTimeMillis());
            client.connect();
            client.setCallback(this);
            client.subscribe(TEMPERATURE_TOPIC, 1);
            messageQueue = new BlockingArrayQueue<>();
        } catch (MqttException e) {
            e.printStackTrace();
        }
        dbClient = new MongoClient();
        db = dbClient.getDatabase(MONGO_DB);

    }

    @Override
    public void close() {
        System.out.println("PressureSpout.close");
    }

    @Override
    public void activate() { /* 1rst call */
        System.out.println("PressureSpout.activate");
    }

    @Override
    public void deactivate() {
        System.out.println("PressureSpout.deactivate");
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
                    for(int i=0; i< values.length; i++) values[i]=values[i].trim();
                    System.out.println("TEMPERATURE : Printing values of topic " + topic + " " + values[0] + " " + values[1]+ " " + values[2]);
                    collector.emit(new Values(values[0], Double.valueOf(values[1]), Long.valueOf(values[2])));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void ack(Object o) {
        System.out.println("PressureSpout.ack");
    }

    @Override
    public void fail(Object o) {
        System.out.println("PressureSpout.fail");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //1 named streams example
        // outputFieldsDeclarer.declareStream(TEMPERATURE_STREAM, new Fields("id", "temperature_value", "timestamp"));
        //2 old way,
        outputFieldsDeclarer.declare(new Fields("id", "temperature_value", "timestamp"));

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
        System.out.println("PressureSpout.deliveryComplete");
    }
}
