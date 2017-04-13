import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import spouts.MessageSpout;


public class HealthMonitorTopology {
    static volatile boolean keepRunning = true;
    public static final String HEALTH_SPOUT = "health-spout";
    public static final String PRESSURE_BOLT= "pressure-bolt";
    public static final String EMERGENCY_BOLT= "emergency-bolt";
    public static final Thread mainThread = Thread.currentThread();

    public static void main(String[] args) throws Exception {
        System.out.println("HealthMonitorTopology.main");
        TopologyBuilder builder  =  new TopologyBuilder();
        /*set spout here */
        builder.setSpout(HEALTH_SPOUT, new MessageSpout(), 2);
        builder.setBolt(PRESSURE_BOLT, new bolts.PressureBolt(), 5).fieldsGrouping(HEALTH_SPOUT, new Fields("id"));
        builder.setBolt(EMERGENCY_BOLT, new bolts.EmergencyBolt(), 1).fieldsGrouping(PRESSURE_BOLT, new Fields("id"));

        Config conf = new Config();
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 10);
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(100000000);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("Shutdown--------------------------");
                    keepRunning = false;
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



}
