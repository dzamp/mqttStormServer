# mqttStormServer  
mqttStormServer to process incoming messages from clients  
## Maven Run Instructions  
1. Install dependencies  
```shell  
mvn install  
```  
2. Package   
```shell  
mvn package
```  
3. Run topology using main class  
```shell
mvn exec:java -Dexec.mainClass="<MainClass>"
```
4. Run jar using instrumentation agent
```shell
java -javaagent:ObjectSizeCalculatorAgent.jar -jar target/monitor-1.0-SNAPSHOT-jar-with-dependencies.jar
```
#### find object size
2. Package the agent into a jar

Once we have compiled our agent class, we need to package it into a jar. This step is slightly fiddly, because we also need to create a manifest file. The latter is simple a text file containing a single line that specifies the agent class. For example, you can create a file called manifest.txt with the following line:

Premain-Class: mypackage.MyAgent

Then, to create the jar, we execute the following command (it's usually worth creating a batch file or shell script with this line in case you need to re-build the agent jar several times):
```shell
jar -cmf manifest.txt agent.jar mypackage/MyAgent.class

jar -cvfm test.jar test.mf -C ./ .


javac -cp ".:javassist-3.20.0-GA.jar" Agent.java ExecutionTimeTransformer.java
 jar -cmf manifest.txt agent.jar Agent.class ExecutionTimeTransformer.class
 java -javaagent:agent.jar -jar target/monitor-1.0-SNAPSHOT-jar-with-dependencies.jar
```
