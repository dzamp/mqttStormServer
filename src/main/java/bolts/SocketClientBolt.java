package bolts;

import javafx.util.Pair;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * Created by jim on 23/5/2017.
 */
public class SocketClientBolt implements IRichBolt {
    protected OutputCollector _collector;
    protected Logger logger;
    protected ArrayList<String> addresses;
    protected int port;
    private ArrayList<SocketChannel> clients = null;


    public SocketClientBolt(ArrayList<String> addresses){
        this.addresses = addresses;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.logger = Logger.getLogger(this.getClass());
        this.clients = new ArrayList<>();
        this.port = ((Long) map.get("SOCKET_CONNECTION_PORT")).intValue();
        for (String s : addresses)
            connect(new InetSocketAddress(s, this.port));
    }

    private void connect(InetSocketAddress address) {
        try {
            clients.add(SocketChannel.open(address));
            logger.info("Client connnected to " + address.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String topic = tuple.getString(0);
        String id = tuple.getString(1);
        List<Pair<? extends Number, Long>> values = (ArrayList<Pair<? extends Number, Long>>) tuple.getValue(2);
        sendValues(topic, id, values);
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

    public void sendValues(String topic, String id, Number value, long timestamp) {
        ByteBuffer buffer = ByteBuffer.wrap((id + "," + String.valueOf(value) + "," + String.valueOf(timestamp)).getBytes());
        send(buffer.toString());
    }

    public void sendValues(String topic, String id, List<Pair<? extends Number, Long>> pairs) {
        String payload = topic + "\n" + id + "\n";
        StringBuilder builder = new StringBuilder(topic + "\n" + id + "\n");
        int i = 0;
        for (i = 0; i < pairs.size(); i++) {
            builder.append(String.valueOf(pairs.get(i).getKey()) + "," + String.valueOf(pairs.get(i).getValue()) + "%");
        }
        //if byuffer has space
        send(builder.toString());
    }

    private void send(String payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload.getBytes());
        clients.forEach(client -> {
            try {
                client.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
