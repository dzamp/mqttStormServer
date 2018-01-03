package timestamp_join;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilePrinterBolt extends BaseRichBolt {
    private PrintWriter writer;
    private OutputCollector collector;
    private TopologyContext context;

    public void setWriter(String fileName) {
        try {
            writer = new PrintWriter(fileName + Time.currentTimeMillis()+ ".txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
           this.context = context;
           this.collector  = collector;
    }

    @Override
    public void execute(Tuple input) {
//        System.out.println("fwdfwef");
        Fields fields = input.getFields();
        Class clazz = input.getClass();
        Map<String,List<Values>> streamValues = (Map<String, List<Values>>) input.getValues().get(0);
        for(String stream: streamValues.keySet()){
            List<Values> vals = streamValues.get(stream);
            for (Values objects : vals) {
                ((Tuple)objects.get(0)).getValueByField("word");
                System.out.println("" + objects.get(0) + " " + objects.get(1));
            }

        }
        System.out.println("dadw");
//        List<Object> objs = (ArrayList<Object>)input.getValue(0);
//        String[] fieldNames = fields.toList().toArray(new String[0]);
//        String value = input.getStringByField(fieldNames[0])+ ", "+ input.getStringByField(fieldNames[1] /*+ ", " + ((Timestamp)input.getValueByField(fieldNames[2])).getTime()*/);
//        writer.println("hey");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //none
    }

    @Override
    public void cleanup() {


    }
}
