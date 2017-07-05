import java.io.IOException;
import java.io.InputStream;

/**
 * Created by jim on 5/7/2017.
 */
public class MongoConnectorProcess {
    public static String jar = "mongo.connector-1.0-SNAPSHOT-jar-with-dependencies.jar";
    public String path = "";
    private InputStream inputStream, errorStream;
    private Process proc = null;
    public MongoConnectorProcess(String path){
        if(!path.equals("")){
            this.path = path;
        }
    }

    public void runJar() {
        try {
            proc = Runtime.getRuntime().exec(path + "java -jar " + this.jar);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(proc!=null){
            inputStream = proc.getInputStream();
            errorStream = proc.getErrorStream();
        }

    }

    public void destroyProcess(){
        if(proc!=null) proc.destroy();
    }

}
