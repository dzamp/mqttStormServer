package windowed_with_field;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.*;

public class TimestampProvidedWindowedBolt extends BaseWindowedBolt {
    long previousInvocation=0;
    Set<String> streamNames = new HashSet<>();
    TopologyContext ctx;
    private OutputCollector collector;
    private TopologyContext context;
    private Map<String,List<String>> streamFieldsMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        Map<GlobalStreamId,Grouping> sources  = context.getThisSources();
        Set<String> streams = context.getThisStreams();
        this.ctx = context;
        this.collector = collector;
        this.context = context;
//        Set<String> streams = context.getThisStreams();

//        context.getComponentOutputFields(context.getThisComponentId(),)
        Map<String,Map<String,List<String>>> map =  context.getThisInputFields();
        streamFieldsMap = new HashMap<>();
        for(String stream : map.keySet()){
            List<String> fields = map.get(stream).get("default");
            streamFieldsMap.put(stream, new ArrayList<>(fields));
        }
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
        Map<String,List<Values>> streamValues = new HashMap<>();
        Set<String> streams = ctx.getThisInputFields().keySet();
        for(String stream: streams) {
            //construct the entries in the map
//            streamFieldsMap.put(stream,new ArrayList<>());
            streamValues.put(stream,new ArrayList<>());
        }
//        for(Tuple tuple: inputWindow.get()){
//            String streamName = tuple.getSourceComponent();
//            Values vals = new Values(tuple.getValues());
//                streamValues.get(tuple.getSourceComponent()).add(new Values(tuple.getValues()));
//        }

        for (Tuple tuple : inputWindow.get()) {
            Values values = (Values) tuple.getValues();
            String sourceComponent= tuple.getSourceComponent();
            streamValues.get(tuple.getSourceComponent()).add((Values) tuple.getValues());
        }
        System.out.println("ehehe");
        streamNames.forEach(s -> System.out.println(s));
//        inputWindow.get().forEach(tuple -> );
        this.collector.emit(new Values(streamValues,streamFieldsMap ));
   }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("values","meta"));
    }
}
