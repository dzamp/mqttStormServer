import javassist.*;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;


public class ExecutionTimeTransformer  implements ClassFileTransformer{

    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer)
            throws IllegalClassFormatException {
        byte[] bytecode = classfileBuffer;
        if (className.equals("bolts/PressureBolt")) {
            try {
                ClassPool cp = ClassPool.getDefault();
                CtClass cc = cp.get("bolts.PressureBolt");
                CtField countOfExecutions = new CtField(CtClass.longType,"countOfExecutions",cc);
                CtField totalTimeElapsed = new CtField(CtClass.longType,"totalTimeElapsed",cc);
                cc.addField(countOfExecutions);
                cc.addField(totalTimeElapsed);
                CtMethod m = cc.getDeclaredMethod("execute");
                m.addLocalVariable("elapsedTime", CtClass.longType);
                m.insertBefore("elapsedTime = System.currentTimeMillis(); countOfExecutions++;");
                m.insertAfter("{elapsedTime = System.currentTimeMillis() - elapsedTime; " +
                        "totalTimeElapsed += elapsedTime; " +
                        "if(countOfExecutions % 100==0){" +
                        "System.out.println(\"Average execution time of execute\" + (double)totalTimeElapsed/countOfExecutions);" +
                        "}}");
                // CtMethod finalize = CtNewMethod.make(" protected void finalize() throws Throwable {System.out.println(\"finalize called\"); " +
                //         "log.info(\"Average execution time of execute\" + (double)totalTimeElapsed/countOfExecutions);  }",cc );
                // ClassFile ccFile = cc.getClassFile();
                // ConstPool constpool = ccFile.getConstPool();
                // AnnotationsAttribute attr = new AnnotationsAttribute(constpool, AnnotationsAttribute.visibleTag);
                // Annotation annot = new Annotation("Override", constpool);
                // attr.addAnnotation(annot);
                // finalize.getMethodInfo().addAttribute(attr);
                // cc.addMethod(finalize);
                bytecode = cc.toBytecode();
                cc.detach();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return bytecode;


    }
}
