package bolts;

import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.storm.shade.org.eclipse.jetty.util.BlockingArrayQueue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.eclipse.paho.client.mqttv3.*;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by jim on 9/7/2017.
 */
public class ReplicationBolt implements IRichBolt {
    protected OutputCollector _collector;
    protected Logger logger;
    protected ArrayList<String> addresses;
    protected int port;
    private ArrayList<SocketChannel> clients = null;
    protected MqttClient client;
    public final static String REPLICATION_TOPIC = "health_monitor/replication";
    public  String MY_IP;
    public  String REPLICATION_IP;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.MY_IP = map.get("MY_IP").toString();
        this.REPLICATION_IP = map.get("REPLICATION_IP").toString();
    }

    @Override
    public void execute(Tuple tuple) {
        String topic = tuple.getString(0);
        String id = tuple.getString(1);
        List<Pair<? extends Number, Long>> values = (ArrayList<Pair<? extends Number, Long>>) tuple.getValue(2);
        String payload = topic + "\n" + id + "\n";
        StringBuilder builder = new StringBuilder(MY_IP + "\n" + topic + "\n" + id + "\n");
        int i = 0;
        MqttClient client = null;
        try {
            client = new MqttClient("tcp://" + this.REPLICATION_IP + ":1883", "Sending"+ Time.currentTimeMillis());
            client.connect();
            client.subscribe(REPLICATION_TOPIC, 1);
        } catch (MqttException e) {
            e.printStackTrace();
        }

        for (i = 0; i < values.size(); i++) {
            builder.append(String.valueOf(values.get(i).getKey()) + "," + String.valueOf(values.get(i).getValue()) + "%");
        }
        MqttMessage message = new MqttMessage(builder.toString().getBytes());
        try {
            client.publish(REPLICATION_TOPIC, message);
        } catch (MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


}
