package bolts;

/**
 * Created by jim on 22/5/2017.
 */

import javafx.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SocketClient {

    private final InetSocketAddress address;
    Logger logger = Logger.getLogger(this.getClass());
    private SocketChannel client = null;

    public SocketClient(InetSocketAddress address) {
        this.address = address;
        connect(address);
    }

    public SocketClient() {
        this.address = new InetSocketAddress("localhost", 8090);
        connect(address);
    }

    private void connect(InetSocketAddress address) {
        try {
            this.client = SocketChannel.open(address);
            logger.info("Client connnected to " + address.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendValues(String topic, String id, Number value, long timestamp) {
        ByteBuffer buffer = ByteBuffer.wrap((id + "," + String.valueOf(value) + "," + String.valueOf(timestamp)).getBytes());
        try {
            client.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public <T extends Number> void sendValues(String topic, String id, List<Pair<T, Long>> pairs) {
        String payload = topic + "\n" + id + "\n";
        StringBuilder builder = new StringBuilder(topic + "\n" + id + "\n");
        int i = 0;
        for (i = 0; i < pairs.size(); i++) {
            builder.append(String.valueOf(pairs.get(i).getKey()) + "," + String.valueOf(pairs.get(i).getValue()) + "%");
            // if (builder.toString().getBytes().length + pairs.get(i).toString().trim().getBytes().length >= /*100**/ 1024) {
            //     send(builder.toString());
            //     builder = new StringBuilder(topic + "\n" + id + "\n");
            // }
        }
        //if byuffer has space
        send(builder.toString());
    }

    private void send(String payload){
        ByteBuffer buffer = ByteBuffer.wrap(payload.getBytes());
        try {
            client.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
