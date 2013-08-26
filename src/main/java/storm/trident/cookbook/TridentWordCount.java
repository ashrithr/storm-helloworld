package storm.trident.cookbook;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

/**
 * Author: ashrith
 * Date: 8/26/13
 * Time: 3:32 PM
 * Desc: Example taken from storm-starter, this example does two things
 *       1. Compute streaming word count from an input stream of sentences
 *       2. Implement queries to get the sum of word counts for a list of words
 */

public class TridentWordCount {

    /**
     * Notes:
     * 1. Operations that read from or write to state (like persistentAggregate and stateQuery) automatically batch
     *    operations to that state.
     * 2. Trident aggregators are heavily optimized. Rather than transfer all tuples for a group to the same machine
     *    and then run the aggregator, Trident will do partial aggregations when possible before sending tuples over
     *    the network. Ex: Count aggregator computes the count on each partition, sends the partial count over the
     *    network, and then sums together all the partial counts to get the total count.
     */

    /*
     * Function takes the "sentence" field and emits a tuple for each word.
     */
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
        /*
         * spout reads an infinite stream of sentences from the following source
         */
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);

        /*
         * TridentTopology object exposes the interface for constructing Trident computations
         */
        TridentTopology topology = new TridentTopology();

        /*
         * TridentState state could be updated by Trident (like in this example), or it could be an independent source
         * of state.
         * newStream()  => Creates a new stream of data in the topology reading from an input source, input sources can
         *                 also be queue brokers like Kestrel or Kafka. Trident keeps track of a small amount of state
         *                 for each input source in Zookeeper, and the "spout1" string here specifies the node in
         *                 Zookeeper where Trident should keep that metadata.
         * each()       => allows to manipulate every Tuple in the batch either by a Filter or a Function, in this case
         *                 applies a function 'Split()' to each tuple in the stream
         * Aggregations => Trident provides functions for doing aggregations across batches and persistently storing
         *                 those aggregations – whether in memory, in Memcached, in Cassandra, or some other store.
         *                 Aggregations such as group by(s), joins, aggregations, functions, filters can be applied.
         *                 In this case the stream is 'grouped' by the 'word' field.
         *
         * The rest of the topology computes word count and keeps the results persistently stored.
         * persistentAggregate() => * knows how to store and update the results of the aggregation in a source of state,
         *                          in this case the word counts are stored in memory.
         *                          * The values stored by persistentAggregate represents the aggregation of all batches
         *                          ever emitted by the stream.
         *                          * persistentAggregate method transforms a Stream into a TridentState object
         */
        TridentState wordCounts =
                topology.newStream("spout1", spout)
                        .parallelismHint(16)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                        .parallelismHint(16);

        /*
         * Trident has first-class functions for querying sources of real-time state. The following implements a low
         * latency distributed query on the word counts. The query takes as input a whitespace separated list of words
         * and return the sum of the counts for those words. These queries are executed just like normal RPC calls,
         * except they are parallelized in the background. The latency for small queries like this are typically around
         * 10ms.
         * topology => TridentTopology object is used to create the DRPC stream, in this case the function name is
         *             'words', function name corresponds to the function name given in the first argument of execute
         *             when using a DRPCClient.
         * Each DRPC request is treated as its own little batch processing job that takes as input a single tuple
         * representing the request.
         * args => The tuple contains one field called "args" that contains the argument provided by the client, in this
         *         case the argument is a whitespace separated list of words (cat the dog jumped).
         * stateQuery() => operator is used to query the TridentState object that the first part of the topology
         *                 generated. stateQuery takes in a source of state – in this case, the word counts computed by
         *                 the other portion of the topology – and a function for querying that state.
         * MapGet() => gets the count for each word
         *
         * Since the DRPC stream is grouped the exact same way as the TridentState was (by the "word" field), each word
         * query is routed to the exact partition of the TridentState object that manages updates for that word.
         *
         * Words that didn't have a count are filtered out via the FilterNull filter and the counts are summed using
         * the Sum aggregator to get the result & Trident automatically sends the result back to the waiting client.
         */
        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
        ;
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if(args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for(int i=0; i<100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
                Thread.sleep(1000);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        }
    }
}

