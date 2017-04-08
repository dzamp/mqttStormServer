package health.monitor;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;

public class WordNormalizer extends BaseBasicBolt {

    public void cleanup() {}

    /**
     * The bolt will receive the line from the
     * words file and process it to Normalize this line
     *
     * The normalize will be put the words in lower case
     * and split the line to get all words in this
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        MqttMessage message = (MqttMessage) input.getValue(0);
        String jsonMessage = message.toString();
        ObjectMapper mapper = new ObjectMapper();

//JSON from file to Object
        HealthMonitorMessage obj= null;
        try {
            obj = mapper.readValue(jsonMessage, HealthMonitorMessage.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        collector.emit(new Values(obj.getValue()));

    }



    /**
     * The bolt will only emit the field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
