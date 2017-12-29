import kafka_example.CountBolt;
import kafka_example.UUIDSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.Bolt;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.FixedTupleSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TestTopology {
    public static final Thread mainThread = Thread.currentThread();
    static volatile boolean keepRunning = true;

    public static void main(String[] args) {
        Config cnf = new Config();
        cnf.setDebug(true);
        CountBolt countBolt = new CountBolt();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("uuid", new UUIDSpout());
        builder.setBolt("counter", new CountBolt()).shuffleGrouping("uuid");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test",cnf , builder.createTopology());
        Utils.sleep(1000);
        List<String> listString = (ArrayList) Arrays.asList(new String[]{"hey","ho","lets","go"});
        listString.forEach(a -> {
            System.out.println(a);
            String[] spliee = a.split(",");
        });
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutdown--------------------------");
                keepRunning = false;
                // mongoConnectorProcess.destroyProcess();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                cluster.killTopology("test");
                cluster.shutdown();
            }
        });
    }

}
