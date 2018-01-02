package windowed_with_field;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TimestampProvidedWindowedBolt extends BaseWindowedBolt {
    long previousInvocation=0;
    Set<String> streamNames = new HashSet<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        Map<GlobalStreamId,Grouping> sources  = context.getThisSources();
        Set<String> streams = context.getThisStreams();
        System.out.println("dawdaw");

    }

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


        for(Tuple t : inputWindow.get()){
            streamNames.add(t.getSourceComponent());
        }

        streamNames.forEach(s -> System.out.println(s));

    }

}
