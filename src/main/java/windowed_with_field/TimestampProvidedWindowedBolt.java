package windowed_with_field;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class TimestampProvidedWindowedBolt extends BaseWindowedBolt {
    long previousInvocation=0;

    @Override
    public void execute(TupleWindow inputWindow) {
//        inputWindow.get().forEach(tuple -> System.out.println(tuple.getString(0)+ ", " +tuple.getLong(1)));

        System.out.println("Sliding window interval =" + (System.currentTimeMillis()- previousInvocation)/1000 );;
        if(previousInvocation ==0) previousInvocation = System.currentTimeMillis();

        Tuple max = inputWindow.getNew().stream().max((o1, o2) ->
                (o1.getLong(1) < o2.getLong(1)) ? -1 : ((o1.getLong(1) == o2.getLong(1)) ? 0 : 1)
        ).get();
        Tuple min =  inputWindow.get().stream().min((o1, o2) ->
                (o1.getLong(1) < o2.getLong(1)) ? -1 : ((o1.getLong(1) == o2.getLong(1)) ? 0 : 1)
        ).get();
        System.out.println("Time elapsed = "+ (max.getLong(1) - min.getLong(1)) );
        System.out.println("Time elapsed in seconds= "+ (max.getLong(1) - min.getLong(1)) / 1000 );
    }

}
