package socket;

/**
 * Created by jim on 22/5/2017.
 */

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SocketClient {

    private final InetAddress address;
    private final int port;
    Logger logger = Logger.getLogger(this.getClass());
    private SocketChannel client;

    public SocketClient(InetAddress address, int port) {
        this.address = address;
        this.port = port;
        InetSocketAddress hostAddress = new InetSocketAddress("localhost", 8090);
        SocketChannel client = null;
        try {
            this.client = SocketChannel.open(hostAddress);
            logger.info("Client connnected to " + address.toString());


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                this.client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    // public void sendValues(String id, int value, long timestamp){
    //
    //     ByteBuffer buffer = ByteBuffer.wrap((id + "," + String.valueOf(value)+ "," + String.valueOf(timestamp)).getBytes());
    //     try {
    //         client.write(buffer);
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }
    //
    // public void sendValues(String id, double value, long timestamp) {
    //     ByteBuffer buffer = ByteBuffer.wrap((id + "," + String.valueOf(value)+ "," + String.valueOf(timestamp)).getBytes());
    //     try {
    //         client.write(buffer);
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }

    public void sendValues(String id, Number value, long timestamp){
        ByteBuffer buffer = ByteBuffer.wrap((id + "," + String.valueOf(value)+ "," + String.valueOf(timestamp)).getBytes());
        try {
            client.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
