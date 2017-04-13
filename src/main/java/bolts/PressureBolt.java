package bolts;

import org.apache.log4j.Logger;
import org.apache.storm.shade.org.eclipse.jetty.io.RuntimeIOException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jim on 11/4/2017.
 */
public class PressureBolt implements IRichBolt {
    OutputCollector _collector;
    int id;
    List<Integer> pressureValues;
    Logger log;
    int count;
    ArrayList<Integer> listofIds;
    int taskId;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        pressureValues = new ArrayList<>();
        listofIds = new ArrayList<>();
        log = Logger.getLogger(this.getClass());
        count = 0;
        taskId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {if (this.count == 0) this.id = /*this.id = tuple.getString(0);*/Integer.valueOf(tuple.getString(0)).intValue();
        int id = /*tuple.getString(0);*/Integer.valueOf(tuple.getString(0));
        listofIds.add(id);
        String pressure_str = tuple.getString(1);
        int pressure = Integer.valueOf(pressure_str);
        pressureValues.add(pressure);
        if (pressure > 86) {
            int first_id = listofIds.get(0);
            for (int i = 1; i < listofIds.size(); i++) {
//                if (first_id != listofIds.get(i)) {
//                    listofIds.forEach(s -> System.out.print(" " + s));
//                    throw new RuntimeIOException("ID IS DIFFERENT!!!!!!!!!!!!!!!!!!++++++++++++++");
//                }
            }
            log.error(" BOLT WITH ID " + taskId + " " + "Pressure for patient id " + id + " has peaked to " + pressure + " please notify a doctor");
            pressureValues.forEach(s -> System.out.print(s + " "));

//            _collector.emit(new Values(id, pressureValues));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "pressure_array"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
