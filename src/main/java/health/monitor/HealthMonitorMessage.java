package health.monitor;

/**
 * Created by jim on 7/4/2017.
 */
public class HealthMonitorMessage {
    public String patient_id;
    public int value;

    public HealthMonitorMessage(String id, int  value){
        this.patient_id = id;
        this.value = value;
    }

    public String getPatient_id() {
        return patient_id;
    }

    public void setPatient_id(String patient_id) {
        this.patient_id = patient_id;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}