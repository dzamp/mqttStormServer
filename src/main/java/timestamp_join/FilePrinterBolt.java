package timestamp_join;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilePrinterBolt extends BaseRichBolt {
    private PrintWriter writer;

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
        setWriter("joinBoltExample");
    }

    @Override
    public void execute(Tuple input) {
        System.out.println("fwdfwef");
        Fields fields = input.getFields();
        List<Object> objs = (ArrayList<Object>)input.getValue(0);
//        String[] fieldNames = fields.toList().toArray(new String[0]);
//        String value = input.getStringByField(fieldNames[0])+ ", "+ input.getStringByField(fieldNames[1] /*+ ", " + ((Timestamp)input.getValueByField(fieldNames[2])).getTime()*/);
        writer.println("hey");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //none
    }

    @Override
    public void cleanup() {
        writer.close();
    }
}
