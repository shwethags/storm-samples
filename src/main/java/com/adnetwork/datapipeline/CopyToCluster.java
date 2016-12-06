package com.adnetwork.datapipeline;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.Testing;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * This topology demonstrates Storm's HiveBolt library to write to Hive.
 */
public class CopyToCluster {

    public static final Character HDFS_PATH_SHORT_OPT = 'p';
    public static final Character TOPIC_NAME_SHORT_OPT = 't';
    public static final Character TOPOLOGY_NAME_SHORT_OPT = 'n';
    public static final Character CLUSTER_SHORT_OPT = 'c';
    public static final Character ENABLE_HOOK_SHORT_OPT = 'k';
    public static final Character KAFKA_URL_SHORT_OPT = 'z';
    public static final Character HDFS_URL_SHORT_OPT = 'h';
    public static final Character HIVE_URL_SHORT_OPT = 'i';
    public static final Character DB_URL_SHORT_OPT = 'd';
    public static final Character TABLE_URL_SHORT_OPT = 't';

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    private static CommandLine parseCommandLineArgs(String[] args) throws ParseException {
        Options options = configureOptions();
        if (args.length == 0) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("storm jar <jarfile> <class> <options>", options);
            System.exit(-1);
        }
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    public static void main(String[] args) throws Exception {

        CommandLine commandLine = parseCommandLineArgs(args);

        TopologyBuilder builder = getTopologyBuilder(commandLine);

        Config conf = new Config();
        conf.setDebug(true);

        String topologyName = "test-topology";
        if (commandLine.hasOption(TOPOLOGY_NAME_SHORT_OPT)) {
            topologyName = commandLine.getOptionValue(TOPOLOGY_NAME_SHORT_OPT);
        }

        if (!commandLine.hasOption(CLUSTER_SHORT_OPT)) {
            if (commandLine.hasOption(ENABLE_HOOK_SHORT_OPT)) {
                conf.putAll(Utils.readDefaultConfig());
                conf.setDebug(true);
                submitToLocal(builder, conf, topologyName, true);
            } else {
                submitToLocal(builder, conf, topologyName, false);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(commandLine.getOptionValue(TOPOLOGY_NAME_SHORT_OPT), conf,
                    builder.createTopology());
        }
    }

    private static TopologyBuilder getTopologyBuilder(CommandLine commandLine) {
        IRichSpout kafkaSpout = getKafkaSpout(commandLine);
        HiveBolt hiveBolt = getHiveBolt(commandLine);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout, 1);
        builder.setBolt("split", new JavaSplitSentence(), 1).shuffleGrouping("kafka-spout");
        builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("hive-bolt", hiveBolt, 1).shuffleGrouping("count");
        return builder;
    }

    private static HdfsBolt getHdfsBolt(CommandLine commandLine) {
        RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(",");
        SyncPolicy countSyncPolicy = new CountSyncPolicy(100);
        FileSizeRotationPolicy fileSizeRotationPolicy = new FileSizeRotationPolicy(5.0f,
                FileSizeRotationPolicy.Units.MB);
        String filePath = "/user/storm/files";
        if (commandLine.hasOption(HDFS_PATH_SHORT_OPT)) {
            filePath = commandLine.getOptionValue(HDFS_PATH_SHORT_OPT);
        }
        DefaultFileNameFormat defaultFileNameFormat = new DefaultFileNameFormat().withPath(filePath);

        String hdfsUrl = commandLine.getOptionValue(HDFS_URL_SHORT_OPT, "hdfs://localhost.localdomain:8020");
        return new HdfsBolt().withFsUrl(hdfsUrl).
                withFileNameFormat(defaultFileNameFormat).
                withRecordFormat(recordFormat).
                withSyncPolicy(countSyncPolicy).
                withRotationPolicy(fileSizeRotationPolicy);
    }

    private static HiveBolt getHiveBolt(CommandLine commandLine) {
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper();
//                .withColumnFields(new Fields(colNames));
        String metastore = commandLine.getOptionValue(HIVE_URL_SHORT_OPT, "thrift://localhost.localdomain:9083");
        String dbName = commandLine.getOptionValue(DB_URL_SHORT_OPT, "default");
        String tableName = commandLine.getOptionValue(TABLE_URL_SHORT_OPT, "table");
        HiveOptions hiveOptions = new
                HiveOptions(metastore, dbName, tableName, mapper);
        return new HiveBolt(hiveOptions);
    }

    private static Options configureOptions() {
        Options options = new Options();
        options.addOption(TOPIC_NAME_SHORT_OPT.toString(), "topic", true, "Kafka topic name");
        options.addOption(HDFS_PATH_SHORT_OPT.toString(), "path", true, "HDFS file path");
        options.addOption(TOPOLOGY_NAME_SHORT_OPT.toString(), "name", true, "Name of the topology");
        options.addOption(CLUSTER_SHORT_OPT.toString(), "cluster", false, "Run on cluster if specified");
        options.addOption(ENABLE_HOOK_SHORT_OPT.toString(), "hook", false, "Enable Storm Atlas Hook if specified");
        options.addOption(KAFKA_URL_SHORT_OPT.toString(), "kafka", true, "Kafka endpoint");
        options.addOption(HDFS_URL_SHORT_OPT.toString(), "hdfs", true, "hdfs endpoint");
        options.addOption(HIVE_URL_SHORT_OPT.toString(), "hive", true, "hive endpoint");
        options.addOption(DB_URL_SHORT_OPT.toString(), "db", true, "hive db");
        options.addOption(TABLE_URL_SHORT_OPT.toString(), "table", true, "hive table");
        return options;
    }

    private static void submitToLocal(TopologyBuilder builder, Config conf, String name, boolean enableHook)
            throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        if (enableHook) {
            conf.put(Config.STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN,
                    "org.apache.atlas.storm.hook.StormAtlasHook");
        }

        conf.setMaxTaskParallelism(3);

        Map<String,Object> localClusterConf = new HashMap<>();
        localClusterConf.put("nimbus-daemon", true);
        ILocalCluster cluster = Testing.getLocalCluster(localClusterConf);

        cluster.submitTopology(name, conf, builder.createTopology());

        Thread.sleep(600000);

        cluster.shutdown();
    }

    private static IRichSpout getKafkaSpout(CommandLine commandLine) {
        String kafkaUrl = commandLine.getOptionValue(KAFKA_URL_SHORT_OPT, "localhost:2181");
        ZkHosts zkHosts = new ZkHosts(kafkaUrl);
        String topicName = "test-topic";
        if (commandLine.hasOption(TOPIC_NAME_SHORT_OPT)) {
            topicName = commandLine.getOptionValue(TOPIC_NAME_SHORT_OPT);
        }
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }

    private static class JavaSplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String sentence = tuple.getString(0);
            String[] words = sentence.split(" ");
            for (String word : words) {
                basicOutputCollector.emit(new Values(word.toLowerCase()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }
}
