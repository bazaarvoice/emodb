package com.bazaarvoice.emodb.table.db.consistency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.rmi.ConnectException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;

public class JmxClient implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxClient.class);

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private JMXConnector jmxc;
    private MBeanServerConnection mbs;

    public JmxClient(String host, int port) throws IOException {
        this(host, port, null, null);
    }

    public JmxClient(String host, int port, String username, String password) throws IOException {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        connect();
    }

    private void connect() throws IOException {
        String url = String.format("service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi", this.host, this.port);
        JMXServiceURL jmxUrl = new JMXServiceURL(url);
        Map<String, Object> env = new HashMap<>();
        if (this.username != null) {
            String[] creds = new String[]{this.username, this.password};
            env.put("jmx.remote.credentials", creds);
        }

        env.put("com.sun.jndi.rmi.factory.socket", this.getRMIClientSocketFactory());
        this.jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        this.mbs = this.jmxc.getMBeanServerConnection();
    }

    private static RMIClientSocketFactory getRMIClientSocketFactory() {
        return Boolean.parseBoolean(System.getProperty("ssl.enable")) ?
                new SslRMIClientSocketFactory() :
                RMISocketFactory.getDefaultSocketFactory();
    }

    /**
     * Gets attribute for matching bean and attribute name.
     */
    public Object getAttribute(ObjectName objectName, String attributeName)
            throws AttributeNotFoundException, InstanceNotFoundException, MBeanException,
            ReflectionException, IOException {
        Object attr = mbs.getAttribute(objectName, attributeName);
        if (attr instanceof javax.management.Attribute) {
            return ((Attribute) attr).getValue();
        }
        return attr;
    }

    @Override
    public void close() throws IOException {
        try {
            this.jmxc.close();
        } catch (ConnectException e) {
            LOGGER.error("Failed to close JMX connection: ", e);
        }
    }
}
