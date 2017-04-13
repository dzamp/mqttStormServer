import health.monitor.HealthMonitorMessage;
import health.monitor.MqttMessageSpout;
import health.monitor.PressureAlert;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("mqtt-message", new MqttMessageSpout(), 1);
        builder.setBolt("exclaim1", new PressureAlert(), 1).shuffleGrouping("mqtt-message");
        builder.setBolt("exclaim2", new ExclamationBolt(), 1).shuffleGrouping("exclaim1");

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
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    public static class ExclamationBolt extends BaseRichBolt {
        Logger log ;
        OutputCollector _collector;
        List<String> messages;
        transient CountMetric _countMetric;
        transient MultiCountMetric _wordCountMetric;
        transient ReducedMetric _wordLengthMeanMetric;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            log = Logger.getLogger(this.getClass());
            this.messages = new ArrayList<String>();
//            initMetrics(context);
        }

        @Override
        public void execute(Tuple tuple) {
            System.out.println("ExclamationBolt.execute");
//      MqttMessage message = (MqttMessage)tuple.getValue(0);
//      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
//            this.messages.add(tuple.getString(0) + "!!!");
            HealthMonitorMessage message = (HealthMonitorMessage) tuple.getValue(0);
            if(message.getMeasurement()>120) log.error("CRITICAL VALUE!!!!!!!");
            _collector.ack(tuple);
            log.info(message.toString());
//            updateMetrics(message.toString());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public void cleanup() {
            messages.forEach(s -> System.out.println(s));
            super.cleanup();
        }

        void initMetrics(TopologyContext context) {
            _countMetric = new CountMetric();
            _wordCountMetric = new MultiCountMetric();
            _wordLengthMeanMetric = new ReducedMetric(new MeanReducer());

            context.registerMetric("execute_count", _countMetric, 5);
            context.registerMetric("word_count", _wordCountMetric, 60);
            context.registerMetric("word_length", _wordLengthMeanMetric, 60);
        }

        void updateMetrics(String word) {
            _countMetric.incr();
            _wordCountMetric.scope(word).incr();
            _wordLengthMeanMetric.update(word.length());
        }
    }
}