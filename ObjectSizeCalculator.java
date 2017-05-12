import java.lang.instrument.Instrumentation;
/**
 * Created by jim on 11/5/2017.
 */

public class ObjectSizeCalculator {
    private static Instrumentation instrumentation;

    public static void premain(String args, Instrumentation inst) {
        System.out.println("heyyeee");
        instrumentation = inst;
    }

    public static long getObjectSize(Object o) {
        return instrumentation.getObjectSize(o);
    }
}