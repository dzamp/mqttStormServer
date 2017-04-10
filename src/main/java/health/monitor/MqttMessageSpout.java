package health.monitor;


import org.apache.log4j.Logger;
import org.apache.storm.shade.org.eclipse.jetty.io.RuntimeIOException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.eclipse.paho.client.mqttv3.*;

import java.util.LinkedList;
import java.util.Map;
import java.util.Stack;

/**
 * Created by jim on 7/4/2017.
 */
public class MqttMessageSpout implements IRichSpout, MqttCallback {
    SpoutOutputCollector collector;
//    Logger log = Logger.getLogger(MqttMessageSpout.class);
    MqttClient client;
    Stack<MqttMessage> messageStack;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void close() {
        System.out.println("MqttMessageSpout.close");
    }

    @Override
    public void activate() { /* 1rst call */
        System.out.println("MqttMessageSpout.activate");
        try {
            client = new MqttClient("tcp://workstation00.raspberryip.com:1883", "Sending");
            client.connect();
            client.setCallback(this);
            client.subscribe("blood_pressure");
            messageStack = new Stack<MqttMessage>();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deactivate() {
        System.out.println("MqttMessageSpout.deactivate");
    }

    @Override
    public void nextTuple() {
        System.out.println("MqttMessageSpout.nextTuple");
     if(!messageStack.isEmpty()){
         collector.emit(new Values(messageStack.pop()), messageStack.pop().hashCode());
     }
    }

    @Override
    public void ack(Object o) {
        System.out.println("MqttMessageSpout.ack");
    }

    @Override
    public void fail(Object o) {
        System.out.println("MqttMessageSpout.fail");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
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
        System.out.println("MqttMessageSpout.messageArrived");
        System.out.println("Message is " +s);
        System.out.println(mqttMessage.toString());
        messageStack.push(mqttMessage);
        nextTuple();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("MqttMessageSpout.deliveryComplete");
    }
}
