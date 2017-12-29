import bolts.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import spouts.OxygenSpout;
import spouts.PressureSpout;
import spouts.TemperatureSpout;

import java.util.ArrayList;
import java.util.Arrays;


public class HealthMonitorTopology {
    public static final String PRESSURE_SPOUT = "pressure-spout";
    public static final String OXYGEN_SPOUT = "oxygen-spout";
    public static final String TEMPERATURE_SPOUT = "temperature-spout";
    public static final String PRESSURE_BOLT = "pressure-bolt";
    public static final String OXYGEN_BOLT = "oxygen-bolt";
    public static final String EMERGENCY_BOLT = "emergency-bolt";
    public static final String TEMPERATURE_BOLT = "temperature-bolt";
    public static final String REPLICA_BOLT = "replica-bolt";
    public static final Thread mainThread = Thread.currentThread();
    static volatile boolean keepRunning = true;

    public static void main(String[] args) throws Exception {
        // MongoConnectorProcess mongoConnectorProcess = new MongoConnectorProcess("");
        // mongoConnectorProcess.runJar();

        PropertyFileLoader propertyFileLoader = new PropertyFileLoader();


        Config conf = new Config();
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 10);
        conf.setDebug(true);
        conf.put("MONGO_DB", propertyFileLoader.getProperty("MONGO_DB"));
        conf.put("PRESSURE_WRITE_THRESHOLD", Integer.valueOf(propertyFileLoader.getProperty("PRESSURE_WRITE_THRESHOLD")));
        conf.put("PRESSURE_EMERGENCY_THRESHOLD", Integer.valueOf(propertyFileLoader.getProperty("PRESSURE_EMERGENCY_THRESHOLD")));
        conf.put("TEMPERATURE_EMERGENCY_THRESHOLD", Double.valueOf(propertyFileLoader.getProperty("TEMPERATURE_EMERGENCY_THRESHOLD")));
        conf.put("TEMPERATURE_WRITE_THRESHOLD", Double.valueOf(propertyFileLoader.getProperty("TEMPERATURE_WRITE_THRESHOLD")));
        conf.put("OXYGEN_WRITE_THRESHOLD", Integer.valueOf(propertyFileLoader.getProperty("OXYGEN_WRITE_THRESHOLD")));
        conf.put("OXYGEN_EMERGENCY_THRESHOLD", Integer.valueOf(propertyFileLoader.getProperty("OXYGEN_EMERGENCY_THRESHOLD")));
        conf.put("REPLICA_REPORT_THRESHOLD", Integer.valueOf(propertyFileLoader.getProperty("REPLICA_REPORT_THRESHOLD")));
        conf.put("EMERGENCY_REPORT_TIME", propertyFileLoader.getProperty("EMERGENCY_REPORT_TIME")); //set hours here
        conf.put("MY_IP",propertyFileLoader.getProperty("MY_IP"));
        conf.put("REPLICATION_IP",propertyFileLoader.getProperty("REPLICATION_IP"));
        conf.put("GUI_IP",propertyFileLoader.getProperty("GUI_IP"));
        TopologyBuilder builder = new TopologyBuilder();

        /*set spout here */
        builder.setSpout(PRESSURE_SPOUT, new PressureSpout(), 1);
        builder.setSpout(OXYGEN_SPOUT, new OxygenSpout(), 1);
        builder.setSpout(TEMPERATURE_SPOUT, new TemperatureSpout(), 1);

        // old way with named streams
        // builder.setBolt(PRESSURE_BOLT, new PressureBolt(),2).fieldsGrouping(PRESSURE_SPOUT, PressureSpout.PRESSURE_STREAM, new Fields("id"));
        builder.setBolt(PRESSURE_BOLT, new PressureBolt(), 2).fieldsGrouping(PRESSURE_SPOUT, new Fields("id"));
        builder.setBolt(OXYGEN_BOLT, new OxygenSaturationBolt(), 2).fieldsGrouping(OXYGEN_SPOUT, new Fields("id"));
        builder.setBolt(TEMPERATURE_BOLT, new TemperatureBolt(), 2).fieldsGrouping(TEMPERATURE_SPOUT, new Fields("id"));

        builder.setBolt(REPLICA_BOLT, new ReplicationBolt(), 2).shuffleGrouping(PRESSURE_BOLT, HealthBolt.REPLICA_REPORT_STREAM);
        builder.setBolt(REPLICA_BOLT + "1", new ReplicationBolt(), 2).shuffleGrouping(OXYGEN_BOLT, HealthBolt.REPLICA_REPORT_STREAM);
        builder.setBolt(REPLICA_BOLT + "2", new ReplicationBolt(), 2).shuffleGrouping(TEMPERATURE_BOLT, HealthBolt.REPLICA_REPORT_STREAM);

        builder.setBolt(EMERGENCY_BOLT, new bolts.EmergencyBolt(), 2).fieldsGrouping(PRESSURE_BOLT, PressureBolt.EMERGENCY_STREAM, new Fields("id"))
                .fieldsGrouping(OXYGEN_BOLT, OxygenSaturationBolt.EMERGENCY_STREAM, new Fields("id"))
                .fieldsGrouping(TEMPERATURE_BOLT, TemperatureBolt.EMERGENCY_STREAM, new Fields("id"));
        // builder.setBolt(EMERGENCY_BOLT + "2", new bolts.EmergencyBolt(), 2).fieldsGrouping(OXYGEN_BOLT, OxygenSaturationBolt.EMERGENCY_STREAM, new Fields("id"));
        // builder.setBolt(EMERGENCY_BOLT + "1", new bolts.EmergencyBolt(), 2).fieldsGrouping(TEMPERATURE_BOLT, TemperatureBolt.EMERGENCY_STREAM, new Fields("id"));

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
}
