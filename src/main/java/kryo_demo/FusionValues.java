package kryo_demo;

public class FusionValues {

    String id;
    int value;
    String topic;
    long timestamp;

    public FusionValues(String id, int value, String topic, long timestamp) {
        this.id = id;
        this.value = value;
        this.topic = topic;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
