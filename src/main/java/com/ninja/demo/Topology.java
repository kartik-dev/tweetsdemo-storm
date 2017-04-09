package com.ninja.demo;

/**
 * Created by kramalingam on 09/04/17.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Topology class that sets up the Storm topology for this sample.
 */
public class Topology {

    static final String TOPOLOGY_NAME = "storm-twitter-word-count";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSampleSpout", new TweeterSpout());
        b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("WordSplitterBolt");
        b.setBolt("WordCounterBolt", new WordCounterBolt(10, 5 * 60, 50)).shuffleGrouping("IgnoreWordsBolt");
        b.setBolt("HDFSPersister", new HDFSPersister()).shuffleGrouping("TwitterSampleSpout");
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });

    }

}