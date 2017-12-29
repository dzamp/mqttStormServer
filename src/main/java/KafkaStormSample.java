import kafka_example.CountBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.MultiScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

//import org.apache.storm.kafka.spout.KafkaSpoutConfig;
//import org.apache.storm.kafka.spout.KafkaSpout;
//import storm.kafka.*;
//import storm.kafka.*;

public class KafkaStormSample {
    public static final Thread mainThread = Thread.currentThread();
    static volatile boolean keepRunning = true;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        PropertyFileLoader propertyFileLoader = new PropertyFileLoader();

        String zkConnString = propertyFileLoader.getProperty("KAFKA_SERVER");
        String topic = propertyFileLoader.getProperty("KAFKA_TOPIC");
        BrokerHosts hosts = new ZkHosts(zkConnString);


        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, propertyFileLoader.getProperty("KAFKA_BROKER"), propertyFileLoader.getProperty("KAFKA_CONSUMER_NAME"));
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;

//        kafkaSpoutConfig.geforceFromStart = true;
//        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        StringKeyValueScheme stringScheme =  new StringKeyValueScheme();
        KeyValueSchemeAsMultiScheme keyValueSchemeAsMultiScheme = new KeyValueSchemeAsMultiScheme(stringScheme);
//        StringKeyValueScheme scheme = new StringKeyValueScheme().deserializeKeyAndValue()
        kafkaSpoutConfig.scheme = keyValueSchemeAsMultiScheme;

//        .deserializeKeyAndValue();
//        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        TopologyBuilder builder = new TopologyBuilder();
//        SpoutConfig configKafka =new  SpoutConfig("localhost:9092","helloworld").build();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));

//        builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("kafka-spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-sample", config, builder.createTopology());
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