package storm.cookbook;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Author: ashrith
 * Date: 8/21/13
 * Time: 8:33 PM
 * Desc: spout essentially emits a stream containing 1 of 2 sentences 'Other Random Word' or 'Hello World' based on
 *       random probability. It works by generating a random number upon construction and then generating subsequent
 *       random numbers to test against the original member variable's value. When it matches "Hello World" is emitted,
 *       during the remaining executions the other sentence is emitted.
 */
public class HelloWorldSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private int referenceRandom;
    private static final int MAX_RANDOM = 10;

    public HelloWorldSpout() {
        final Random rand = new Random();
        referenceRandom = rand.nextInt(MAX_RANDOM);
    }

    /*
     * declareOutputFields() => you need to tell the Storm cluster which fields this Spout emits within the
     *  declareOutputFields method.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    /*
     * open() => The first method called in any spout is 'open'
     *           TopologyContext => contains all our topology data
     *           SpoutOutputCollector => enables us to emit the data that will be processed by the bolts
     *           conf => created in the topology definition
     */
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /*
     * nextTuple() => Storm cluster will repeatedly call the nextTuple method which will do all the work of the spout.
     *  nextTuple() must release the control of the thread when there is no work to do so that the other methods have
     *  a chance to be called.
     */
    @Override
    public void nextTuple() {
        final Random rand = new Random();
        int instanceRandom = rand.nextInt(MAX_RANDOM);
        if(instanceRandom == referenceRandom){
            collector.emit(new Values("Hello World"));
        } else {
            collector.emit(new Values("Other Random Word"));
        }
    }
}
