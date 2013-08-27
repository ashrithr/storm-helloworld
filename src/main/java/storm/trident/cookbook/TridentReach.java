package storm.trident.cookbook;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;

import java.util.*;

/**
 * Author: ashrith
 * Date: 8/26/13
 * Time: 4:40 PM
 * Desc: Pure DRPC topology that computes the reach of a URL on demand. Reach is the number of unique people exposed to
 *       a URL on Twitter. This topology will read from two sources of state. One database maps URLs to a list of people
 *       who tweeted that URL. The other database maps a person to a list of followers for that person.
 */
public class TridentReach {
    /*
     * simulate database data
     */
    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
        put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
        put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
        put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
    }};

    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
    }};

    public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {
        public static class Factory implements StateFactory {
            Map _map;

            public Factory(Map map) {
                _map = map;
            }

            @Override
            public State makeState(Map conf, int partitionIndex, int numPartitions) {
                return new StaticSingleKeyMapState(_map);
            }
        }

        Map _map;

        public StaticSingleKeyMapState(Map map) {
            _map = map;
        }

        @Override
        public List<Object> multiGet(List<List<Object>> keys) {
            List<Object> ret = new ArrayList();
            for(List<Object> key: keys) {
                Object singleKey = key.get(0);
                ret.add(_map.get(singleKey));
            }
            return ret;
        }

    }

    /*
     * 'One' is a combiner aggregator, which knows how to do partial aggregations before transferring tuples over the
     * network to maximize efficiency. Built in 'Sum' is also defined as a combiner aggregator, so the global sum done
     * at the end of the topology will be very efficient.
     */
    public static class One implements CombinerAggregator<Integer> {
        @Override
        public Integer init(TridentTuple tuple) {
            return 1;
        }

        @Override
        public Integer combine(Integer val1, Integer val2) {
            return 1;
        }

        @Override
        public Integer zero() {
            return 1;
        }
    }

    /*
     * takes in a list and expands the list objects as tuples
     */
    public static class ExpandList extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            List l = (List) tuple.getValue(0);
            if(l!=null) {
                for(Object o: l) {
                    collector.emit(new Values(o));
                }
            }
        }

    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();

        /*
         * TridentState objects representing each external database using the newStaticState method, these can then be
         * queried in the topology. Like all sources of state, queries to these databases will be automatically batched
         * for maximum efficiency
         */
        TridentState urlToTweeters = topology.newStaticState(new StaticSingleKeyMapState.Factory(TWEETERS_DB));
        TridentState tweetersToFollowers = topology.newStaticState(new StaticSingleKeyMapState.Factory(FOLLOWERS_DB));

        /*
         * Topology Definition
         * First, 'urlToTweeters' database is queried to get all the list of people who tweeted the URL for this
         *  request, which returns a list so the 'ExpandList()' function is invoked to create a tuple for each tweeter
         * Next, the followers for each tweeter must be fetched. It's important that this step be parallelized,
         *  so 'shuffle' is invoked to evenly distribute the tweeters among all workers for the topology
         * Then, the followers database is queried to get the list of followers for each tweeter, this portion of
         *  the topology is given a large parallelism since this is the most intense portion of the computation
         * Next, the set of followers is uniqued and counted which is done in 2 steps
         *  - a "group by" is done on the batch by "follower", running the "One" aggregator on each group, "One"
         *    aggregator simply emits a single tuple containing the number one for each group.
         *  - the ones are summed together to get the unique count of the followers set.
         */
        topology.newDRPCStream("reach", drpc)
                .stateQuery(urlToTweeters, new Fields("args"), new MapGet(), new Fields("tweeters"))
                .each(new Fields("tweeters"), new ExpandList(), new Fields("tweeter"))
                .shuffle()
                .stateQuery(tweetersToFollowers, new Fields("tweeter"), new MapGet(), new Fields("followers"))
                .each(new Fields("followers"), new ExpandList(), new Fields("follower"))
                .groupBy(new Fields("follower"))
                .aggregate(new One(), new Fields("one"))
                .aggregate(new Fields("one"), new Sum(), new Fields("reach"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        LocalDRPC drpc = new LocalDRPC();

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("reach", conf, buildTopology(drpc));

        Thread.sleep(2000);

        System.out.println("REACH: " + drpc.execute("reach", "aaa"));
        System.out.println("REACH: " + drpc.execute("reach", "foo.com/blog/1"));
        System.out.println("REACH: " + drpc.execute("reach", "engineering.twitter.com/blog/5"));


        cluster.shutdown();
        drpc.shutdown();
    }
}