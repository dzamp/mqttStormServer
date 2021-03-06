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
public class PressureSpout implements IRichSpout, MqttCallback {
    //    Logger log = Logger.getLogger(PressureSpout.class);
    public final static String PRESSURE_STREAM = "pressureStream";
    public final static String PRESSURE_TOPIC = "health_monitor/blood_pressure";
    protected SpoutOutputCollector collector;
    protected MqttClient client;
    protected BlockingQueue<Pair<String,MqttMessage>> messageQueue; //A key value store of a topic, MqttMessage


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
//            client = new MqttClient("tcp://workstation00.raspberryip.com:1883", "Sending"+ Time.currentTimeMillis()); //persistence error?
            client = new MqttClient("tcp://localhost:1883", "Sending"+ Time.currentTimeMillis());
            client.connect();
            client.setCallback(this);
            client.subscribe(PRESSURE_TOPIC, 1);
            messageQueue = new BlockingArrayQueue<>();
        } catch (MqttException e) {
            e.printStackTrace();
        }
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
                    System.out.println("PRESSURE : Printing values of topic " + topic + " " + values[0] + " " + values[1]+ " " + values[2]);
                    collector.emit(new Values(values[0], Integer.valueOf(values[1]), Long.valueOf(values[2])));
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
        // outputFieldsDeclarer.declareStream(PRESSURE_STREAM, new Fields("id", "pressure_value", "timestamp"));
        //2 old way,
        outputFieldsDeclarer.declare(new Fields("id", "pressure_value", "timestamp"));

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
