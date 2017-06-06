package opt;

/**
 * Created by jim on 29/5/2017.
 */
public interface ThresholdInterface<T extends Number> {
    boolean CompareDelta(T currentValue, T lastValue);
    boolean EmergencyCondition(T currentValue);
}
