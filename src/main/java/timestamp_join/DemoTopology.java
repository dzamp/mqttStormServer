package timestamp_join;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class DemoTopology {

    public static final Thread mainThread = Thread.currentThread();
    static volatile boolean keepRunning = true;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
//        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("uuid-spout", new RandomWordSpout(new String[]{"uuid", "timestamp"}), 1);
        builder.setSpout("word-spout", new RandomWordSpout(), 1);

        JoinBolt joinBolt = new JoinBolt("uuid-spout", "timestamp")
                .join("word-spout", "timestamp", "uuid-spout")
                .select("uuid,word,timestamp")
                .withTumblingWindow(BaseWindowedBolt.Count.of(10));


        builder.setBolt("join", joinBolt,1)
                .fieldsGrouping("uuid-spout",new Fields("timestamp"))
                .fieldsGrouping("word-spout",new Fields("timestamp"));

        builder.setBolt("fileWriter",new FilePrinterBolt(),1).globalGrouping("join");
//        builder.setBolt("join",

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
