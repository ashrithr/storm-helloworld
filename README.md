##HelloWorld for storm

This project contains a pom.xml which would effectively build the project including dependencies

To execute the HelloWorld topology

```shell
mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.cookbook.HelloWorldTopology
```

To execute the TirdentWordCount topology

```shell
mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.trident.cookbook.TridentWordCount
```

To execute the TridentReach topology

```shell
mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.trident.cookbook.TridentReach
```