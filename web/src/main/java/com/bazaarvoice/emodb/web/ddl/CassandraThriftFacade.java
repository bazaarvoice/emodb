package com.bazaarvoice.emodb.web.ddl;

import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;

final class CassandraThriftFacade implements Closeable {
    private static final Logger _log = LoggerFactory.getLogger(CassandraThriftFacade.class);

    /** Note: <code>seeds</code> may be a single host or comma-delimited list. */
    public static CassandraThriftFacade forSeedsAndPort(String seeds, int defaultPort) {
        final String seed = seeds.contains(",") ? seeds.substring(0, seeds.indexOf(',')) : seeds;
        HostAndPort host = HostAndPort.fromString(seed).withDefaultPort(defaultPort);
        return new CassandraThriftFacade(new TFramedTransport(new TSocket(host.getHostText(), host.getPort())));
    }

    private static final String CQL_VERSION = "3.0.0";

    private final TFramedTransport _transport;
    private final Cassandra.Client _client;

    public CassandraThriftFacade(TFramedTransport transport) {
        _transport = transport;
        _client = new Cassandra.Client(new TBinaryProtocol(_transport));
        try {
            _transport.open();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close() {
        try {
            _transport.close();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    // Note this returns the Thrift protocol version (eg. "19.36.2"), not the Cassandra version (eg. "1.2.18").
    public String describeVersion() {
        try {
            return _client.describe_version();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Nullable
    public KsDef describeKeyspace(String keyspace) {
        try {
            return _client.describe_keyspace(keyspace);
        } catch (Exception e) {
            return null;  // Does not exist
        }
    }

    public void systemAddKeyspace(KsDef keyspaceDefinition) {
        try {
            _client.system_add_keyspace(keyspaceDefinition);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public void systemUpdateKeyspace(KsDef keyspaceDefinition) {
        try {
            _client.system_update_keyspace(keyspaceDefinition);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /** @deprecated Remove once Cassandra 1.1 support is no longer necessary. */
    public void executeCql3Script_1_1(String script) {
        try {
            _client.set_cql_version(CQL_VERSION);
            for (String cqlStatement : toCqlStatements(script)) {
                if (StringUtils.isNotBlank(cqlStatement)) {
                    cqlStatement += ";";
                    _log.info("executing cql statement: " + cqlStatement);
                    _client.execute_cql_query(ByteBuffer.wrap(cqlStatement.getBytes("UTF-8")), Compression.NONE);
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public void executeCql3Script(String script) {
        try {
            for (String cqlStatement : toCqlStatements(script)) {
                if (StringUtils.isNotBlank(cqlStatement)) {
                    cqlStatement += ";";
                    _log.info("executing cql3 statement: " + cqlStatement);
                    _client.execute_cql3_query(ByteBuffer.wrap(cqlStatement.getBytes("UTF-8")), Compression.NONE, ConsistencyLevel.LOCAL_QUORUM);
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static String[] toCqlStatements(String script) {
        return StringUtils.stripAll(StringUtils.split(script, ";"));
    }
}