package health.monitor;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.storm.shade.org.eclipse.jetty.io.RuntimeIOException;
import org.apache.storm.shade.org.eclipse.jetty.util.BlockingArrayQueue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.eclipse.paho.client.mqttv3.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jim on 7/4/2017.
 */
public class MqttMessageSpout implements IRichSpout, MqttCallback {
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
            client = new MqttClient("tcp://workstation00.raspberryip.com:1883", "Sending");
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
        while(true/*!messageQueue.isEmpty()*/) {
            MqttMessage message = null;
            try {
                message = messageQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ObjectMapper mapper =  new ObjectMapper();
            HealthMonitorMessage healthMessage = null;
            try {
                healthMessage = mapper.readValue(message.toString(), HealthMonitorMessage.class);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if(healthMessage!=null) collector.emit(new Values(healthMessage));
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
        outputFieldsDeclarer.declare(new Fields("word"));

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
        System.out.println("Topic is " + s);
        messageQueue.put(mqttMessage);//.push(mqttMessage);
//        nextTuple();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("MessageSpout.deliveryComplete");
    }
}
