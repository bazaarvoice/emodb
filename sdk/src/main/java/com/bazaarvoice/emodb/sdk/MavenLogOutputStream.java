package com.bazaarvoice.emodb.sdk;


import org.apache.commons.exec.LogOutputStream;
import org.apache.maven.plugin.logging.Log;

final class MavenLogOutputStream extends LogOutputStream {

    public static final int ERROR = 0;
    public static final int INFO = 1;

    private final Log log;

    public MavenLogOutputStream(Log log, int level) {
        super(level);
        this.log = log;
    }

    protected void processLine(String line, int level) {
        if (level == INFO) {
            log.info(line);
        } else if (level == ERROR) {
            log.error(line);
        } else {
            throw new IllegalStateException();
        }
    }
}
