package bolts;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.*;


/**
 * Created by jim on 11/4/2017.
 */
public class EmergencyBolt implements IRichBolt {
    protected MongoClient dbClient;
    protected MongoDatabase db;
    protected int reportFromTime;
    OutputCollector _collector;
    public final static String GUI_TOPIC = "health_monitor/gui";
    public String GUI_IP;
    MqttClient client = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        dbClient = new MongoClient();
        db = dbClient.getDatabase("health_monitor");
        reportFromTime = Integer.valueOf((String) map.get("EMERGENCY_REPORT_TIME"));
        this.GUI_IP = map.get("GUI_IP").toString();
        client = null;
        try {
            client = new MqttClient("tcp://" + this.GUI_IP + ":1883", "Sending"+ Time.currentTimeMillis());
            client.connect();
            // client.subscribe(GUI_TOPIC, 1);
        } catch (MqttException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        List<Object> values = tuple.getValues();
        ArrayList<Pair<? extends Number, Long>> patientValues = (ArrayList<Pair<? extends Number, Long>>) tuple.getValue(1);
        String id = tuple.getString(0);
        System.out.println("PATIENT WITH ID " + id + " has reached critical levels, assume action ");
        List<String> userCollections = returnUserCollections(id);
        sendEmergencyValueOnlytoGui(patientValues, userCollections, tuple);
    }

    private void sendEmergencyValueOnlytoGui(ArrayList<Pair<? extends Number, Long>> patientValues, List<String> userCollections, Tuple tuple) {
        long queryFromTimestamp = patientValues.get(patientValues.size() - 1).getValue() - 60 * reportFromTime;
        Number n = null;
        String output = "";
        n = (Number) tuple.getValueByField("emergency_value");
        for (int i =0; i< patientValues.size(); i++) {
            String pressure_value = String.valueOf(patientValues.get(i).getKey());
            String num = String.valueOf(n);
            if (num.equals(pressure_value)) {
                output = output.concat(tuple.getValueByField("topic") + "%" + tuple.getValue(0).toString() + "%" + num + "%" + patientValues.get(i).getValue() + "\n");
            }
        }

        MqttMessage message = new MqttMessage(output.getBytes());
        try {
            client.publish(GUI_TOPIC, message);
        } catch (MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }

    }

    private List<String> returnUserCollections(final String id) {
        List<String> userCollections = new ArrayList<>();
        MongoIterable<String> collections = db.listCollectionNames();
        for (String cName : collections) {
            if (cName.contains(id))
                userCollections.add(cName.trim());
        }
        return userCollections;
    }

    private long calculatePreviousTimestamp() {
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+3"));
        cal.add(Calendar.HOUR_OF_DAY, -reportFromTime);
        Date date = cal.getTime();
        return date.getTime();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("emergency"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
