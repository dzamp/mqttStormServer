package windowed_with_field;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;
import timestamp_join.FilePrinterBolt;

public class DemoWindowTimestampTopology {

    public static final Thread mainThread = Thread.currentThread();
    static volatile boolean keepRunning = true;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
//        config.setDebug(true);
//        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        config.setMessageTimeoutSecs(60);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("uuid-spout", new RandomWordSpout(new String[]{"uuid", "timestamp"}), 1);
        builder.setSpout("word-spout", new RandomWordSpout(), 1);

        BaseWindowedBolt windowBolt = new TimestampProvidedWindowedBolt()
                .withTimestampField("timestamp")
                .withWindow(BaseWindowedBolt.Duration.seconds(30), BaseWindowedBolt.Duration.seconds(10));


        builder.setBolt("window", windowBolt,1)
                .shuffleGrouping("uuid-spout")
                .shuffleGrouping("word-spout");

        builder.setBolt("fileWriter",new FilePrinterBolt(),1).shuffleGrouping("window");
////        builder.setBolt("join",

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("demo-sample", config, builder.createTopology());
        Utils.sleep(1000);
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
                cluster.killTopology("kafka-sample");
                cluster.shutdown();
            }
        });
    }
}
