import java.lang.instrument.Instrumentation;


public class Agent  {
    private static Instrumentation instrumentation;

    public static void premain(String args, Instrumentation inst) {
        System.out.println("heyyeee");

        inst.addTransformer(new ExecutionTimeTransformer());
        instrumentation = inst;
    }

    public static long getObjectSize(Object o) {
        return instrumentation.getObjectSize(o);
    }

}