

import kryo_demo.CustomSerializer;
import kryo_demo.FusionValues;
import kryo_demo.KryoBolt;
import kryo_demo.RandomNumberSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class KryoTopology {


    public static void main(String[] args) {
        Config conf = new Config();
        conf.registerSerialization(FusionValues.class, CustomSerializer.class);
        conf.setFallBackOnJavaSerialization(false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("fusion-spout", new RandomNumberSpout());
        builder.setBolt("kryo-bolt",new KryoBolt()).shuffleGrouping("fusion-spout");
        StormTopology topology = builder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("Kryo test", conf, topology);

        Utils.sleep(20000);

        localCluster.shutdown();
    }


}
