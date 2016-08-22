package com.bazaarvoice.emodb.sdk;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;

class StdoutRedirector extends Thread {
    private static final int BUFFER_SIZE = 2048;

    private final Reader in;
    private final OutputStream out;

    private volatile boolean stopped = false;

    StdoutRedirector(Reader in, OutputStream out) {
        this.in = in;
        this.out = out;
        setDaemon(true);
    }

    public void stopIt() {
        stopped = true;
        interrupt();
    }

    public void run() {
        try {
            char[] buffer = new char[BUFFER_SIZE];
            int count;
            while ((count = in.read(buffer, 0, BUFFER_SIZE)) >= 0) {
                if (stopped) {
                    break;
                }
                out.write(new String(buffer).getBytes(), 0, count);
                out.flush();
            }
        } catch (Exception e) { // generally, we expect IOException or InterruptedException
            if (!stopped) {
                throw new RuntimeException("unexpected", e);
            }
        } finally {
            if (out != null) {
                try {
                    out.flush();
                } catch (IOException e) { // silent
                }
            }
        }
    }
}