package com.bazaarvoice.emodb.sdk;

import com.google.common.collect.Lists;
import org.apache.curator.test.TestingServer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class CrossMojoState {
    private static final String ZOOKEEPER_TESTING_SERVER = "emo.zookeeperTestingServer";
    private static final String CASSANDRA_STOP_PORT = "emo.cassandraStopPort";
    private static final String EMO_PROCESSES = "emo.processes";

    @SuppressWarnings("unchecked")
    public static void putZookeeperTestingServer(TestingServer emoProcess, Map pluginContext) {
        pluginContext.put(ZOOKEEPER_TESTING_SERVER, emoProcess);
    }

    @SuppressWarnings("unchecked")
    public static void putCassandraStopPort(int cassandraStopPort, Map pluginContext) {
        pluginContext.put(CASSANDRA_STOP_PORT, cassandraStopPort);
    }

    @SuppressWarnings("unchecked")
    public static void addEmoProcess(EmoExec emoProcess, Map pluginContext) {
        List<EmoExec> processes = (List<EmoExec>) pluginContext.get(EMO_PROCESSES);
        if (processes == null) {
            pluginContext.put(EMO_PROCESSES, processes = Lists.newArrayList());
        }
        processes.add(emoProcess);
    }

    @SuppressWarnings("unchecked")
    public static List<EmoExec> getEmoProcesses(Map pluginContext) {
        List<EmoExec> processes = (List<EmoExec>) pluginContext.get(EMO_PROCESSES);
        return Optional.ofNullable(processes).orElse(Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    public static TestingServer getZookeeperTestingServer(Map pluginContext) {
        return (TestingServer) pluginContext.get(ZOOKEEPER_TESTING_SERVER);
    }

    @SuppressWarnings("unchecked")
    public static int getCassandraStopPort(Map pluginContext) {
        return (Integer) pluginContext.get(CASSANDRA_STOP_PORT);
    }
}
