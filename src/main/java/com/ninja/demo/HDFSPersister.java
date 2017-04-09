package com.ninja.demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.bolt.HdfsBolt;

import java.util.Map;

/**
 * Created by kramalingam on 09/04/17.
 */
public class HDFSPersister extends BaseRichBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        // Use "|" instead of "," for field delimiter:
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");
        // sync the filesystem after every 1k tuples:
        SyncPolicy syncPolicy = new CountSyncPolicy(100);

        // Rotate files when they reach 5MB:
        FileRotationPolicy rotationPolicy =
                new FileSizeRotationPolicy(5.0f, Units.MB);

        // Write records to <user>/stock-ticks/ directory in HDFS:
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("stock-ticks/");

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://localhost:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}