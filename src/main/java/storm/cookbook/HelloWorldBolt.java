package storm.cookbook;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Author: ashrith
 * Date: 8/26/13
 * Time: 11:48 AM
 * Desc: This bolt will consume the produced Tuples from HelloWorldSpout and implement the required counting logic
 */
public class HelloWorldBolt extends BaseRichBolt {
    private int myCount = 0;

    /*
     * prepare() => on create
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    /*
     * execute() => most important method in the bolt is execute(Tuple input), which is called once per tuple received
     *  the bolt may emit several tuples for each tuple received
     */
    @Override
    public void execute(Tuple tuple) {
        String test = tuple.getStringByField("sentence");
        if(test == "Hello World"){
            myCount++;
            System.out.println("Found a Hello World! My Count is now: " + Integer.toString(myCount));
        }
    }

    /*
     * declareOutputFields => This bolt emits nothing hence no body for declareOutputFields()
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
