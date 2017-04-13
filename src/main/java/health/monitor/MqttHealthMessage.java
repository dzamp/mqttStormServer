package health.monitor;

import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Created by jim on 11/4/2017.
 */
public class MqttHealthMessage extends MqttMessage {
    private String topic;

    public  MqttHealthMessage(String topic) {
        super();
        this.topic= topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }



}
