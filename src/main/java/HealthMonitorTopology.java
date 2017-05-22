import bolts.OxygenSaturationBolt;
import bolts.PressureBolt;
import bolts.TemperatureBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import spouts.OxygenSpout;
import spouts.PressureSpout;
import spouts.TemperatureSpout;


public class HealthMonitorTopology {
    static volatile boolean keepRunning = true;
    public static final String PRESSURE_SPOUT = "pressure-spout";
    public static final String OXYGEN_SPOUT = "oxygen-spout";
    public static final String TEMPERATURE_SPOUT = "temperature-spout";
    public static final String PRESSURE_BOLT= "pressure-bolt";
    public static final String OXYGEN_BOLT= "oxygen-bolt";
    public static final String EMERGENCY_BOLT= "emergency-bolt";
    public static final String TEMPERATURE_BOLT= "temperature-bolt";
    public static final Thread mainThread = Thread.currentThread();



    public static void main(String[] args) throws Exception {
        TopologyBuilder builder  =  new TopologyBuilder();
        /*set spout here */
        builder.setSpout(PRESSURE_SPOUT, new PressureSpout(), 1);
        // builder.setSpout(OXYGEN_SPOUT, new OxygenSpout(), 1);
        builder.setSpout(TEMPERATURE_SPOUT, new TemperatureSpout(), 1);
        // old way with named streams
        // builder.setBolt(PRESSURE_BOLT, new PressureBolt(),2).fieldsGrouping(PRESSURE_SPOUT, PressureSpout.PRESSURE_STREAM, new Fields("id"));
        builder.setBolt(PRESSURE_BOLT, new PressureBolt(),2).fieldsGrouping(PRESSURE_SPOUT, new Fields("id"));
        // builder.setBolt(OXYGEN_BOLT, new OxygenSaturationBolt(),2).fieldsGrouping(OXYGEN_SPOUT, new Fields("id"));
        builder.setBolt(TEMPERATURE_BOLT, new TemperatureBolt(),1).fieldsGrouping(TEMPERATURE_SPOUT, new Fields("id"));
//        builder.setBolt(PRESSURE_BOLT, new bolts.PressureBolt(), 5).fieldsGrouping(PRESSURE_SPOUT, new Fields("id"));
//        builder.setBolt(OXYGEN_BOLT, new bolts.OxygenSaturationBolt(), 5).fieldsGrouping(PRESSURE_SPOUT, new Fields("id"));
        builder.setBolt(EMERGENCY_BOLT, new bolts.EmergencyBolt(), 2).fieldsGrouping(TEMPERATURE_BOLT, new Fields("id"));
        builder.setBolt("emergency-pressure-bolt", new bolts.EmergencyBolt(), 2).fieldsGrouping(PRESSURE_BOLT, new Fields("id"));

        Config conf = new Config();
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 10);
        conf.setDebug(true);
        conf.put("MONGO_DB","health_monitor");
        conf.put("PRESSURE_WRITE_THRESHOLD",2);
        conf.put("PRESSURE_EMERGENCY_THRESHOLD",120);

        conf.put("TEMPERATURE_EMERGENCY_THRESHOLD",38.0);
        conf.put("TEMPERATURE_WRITE_THRESHOLD",0.5);

        conf.put("OXYGEN_WRITE_THRESHOLD",0.5);
        conf.put("OXYGEN_EMERGENCY_THRESHOLD",97);

        conf.put("EMERGENCY_REPORT_TIME","24"); //set hours here

        // System.out.println("------------------------" + Agent.getObjectSize(conf));

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
