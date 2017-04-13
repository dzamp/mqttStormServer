package spouts;


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
    SpoutOutputCollector collector;
    //    Logger log = Logger.getLogger(MessageSpout.class);
    MqttClient client;
    Stack<MqttMessage> messageStack;
    BlockingQueue<MqttMessage> messageQueue;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.out.println("MessageSpout.open");
        this.collector = spoutOutputCollector;
        try {
            client = new MqttClient("tcp://workstation00.raspberryip.com:1883", "Sending"+ Time.currentTimeMillis()); //persistence error?
            client.connect();
            client.setCallback(this);
            client.subscribe("blood_pressure");
            messageQueue = new BlockingArrayQueue<MqttMessage>();
        } catch (MqttException e) {
            e.printStackTrace();
        }
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
            try {
                message = messageQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (message != null) {
                    String[] values = message.toString().split(",");
//                    for(int i=0; i< values.length; i++) values[i] = values[i].trim();
                    System.out.println("Printing values " + values[0] + " " + values[1]);
                    collector.emit(new Values(values[0],values[1]));
                }
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
//        outputFieldsDeclarer.declare(new Fields("word"));
        outputFieldsDeclarer.declare(new Fields("id", "pressure_value"));

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
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
//        System.out.println("MessageSpout.messageArrived");
//        System.out.println("Message is " +s);
//        System.out.println(mqttMessage.toString());
//        System.out.println("String s is " + s);
//        System.out.println("Topic is " + s);
        messageQueue.put(mqttMessage);//.push(mqttMessage);
//        nextTuple();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("MessageSpout.deliveryComplete");
    }
}
