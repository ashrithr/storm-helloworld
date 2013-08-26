package storm.cookbook;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Author: ashrith
 * Date: 8/26/13
 * Time: 12:03 PM
 * Desc: setup the topology and submit it to either a local of remote Storm cluster depending on the arguments
 *       passed to the main method.
 */
public class HelloWorldTopology {
    /*
     * main class in which to define the topology and a LocalCluster object (enables you to test and debug the
     * topology locally). In conjunction with the Config object, LocalCluster allows you to try out different
     * cluster configurations.
     *
     * Create a topology using 'TopologyBuilder' (which will tell storm how the nodes area arranged and how they
     * exchange data)
     * The spout and the bolts are connected using 'ShuffleGroupings'
     *
     * Create a 'Config' object containing the topology configuration, which is merged with the cluster configuration
     * at runtime and sent to all nodes with the prepare method
     *
     * Create and run the topology using 'createTopology' and 'submitTopology'
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomHelloWorld", new HelloWorldSpout(), 10);
        builder.setBolt("HelloWorldBolt", new HelloWorldBolt(), 1).shuffleGrouping("randomHelloWorld");
        Config conf = new Config();
        conf.setDebug(true);
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
