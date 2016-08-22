package com.bazaarvoice.emodb.sdk;

import com.google.common.collect.Lists;
import org.apache.maven.plugin.logging.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class EmoExec {
    private Process process = null;
    private Set<StdoutRedirector> redirectors = new HashSet<>();
    private int maxMemoryMegabytes = 1000;
    private int debugPort = -1;
    private boolean suspendDebugOnStartup = false;
    private File emoLogFile = null;

    public void setMaxMemoryMegabytes(int maxMemoryMegabytes) {
        this.maxMemoryMegabytes = maxMemoryMegabytes;
    }

    public void setDebugPort(int debugPort) {
        this.debugPort = debugPort;
    }

    public void setSuspendDebugOnStartup(boolean suspendDebugOnStartup) {
        this.suspendDebugOnStartup = suspendDebugOnStartup;
    }

    public void setEmoLogFile(File emoLogFile) {
        this.emoLogFile = emoLogFile;
    }

    public void execute(File workingDirectory, Log log, String... args) {
        final ProcessBuilder pb = new ProcessBuilder();
        log.info("Using working directory for emo: " + workingDirectory);
        pb.directory(workingDirectory);
        pb.command(buildCommandLineArguments(args));
        try {
            process = pb.start();
            pumpOutputToLog(process, log);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> buildCommandLineArguments(String... args) {
        final List<String> commandLineArguments = Lists.newArrayList();
        commandLineArguments.add("java");
        commandLineArguments.add("-Xmx" + maxMemoryMegabytes + "m");
        if (debugPort > 0) {
            commandLineArguments.add("-Xdebug");
            final String suspend = suspendDebugOnStartup ? "y" : "n";
            commandLineArguments.add("-Xrunjdwp:transport=dt_socket,server=y,suspend=" + suspend + ",address=" + debugPort);
        }
        commandLineArguments.add("-Djava.awt.headless=true");
        commandLineArguments.add("-jar");
        commandLineArguments.add("emodb.jar");
        Collections.addAll(commandLineArguments, args);
        return commandLineArguments;
    }

    private void pumpOutputToLog(Process process, Log mavenLog) {
        if (emoLogFile == null) {
            // pump to maven log
            redirectors.add(new StdoutRedirector(new InputStreamReader(process.getInputStream()), new MavenLogOutputStream(mavenLog, MavenLogOutputStream.INFO)));
            redirectors.add(new StdoutRedirector(new InputStreamReader(process.getErrorStream()), new MavenLogOutputStream(mavenLog, MavenLogOutputStream.ERROR)));
        } else {
            // pump to file log
            final FileOutputStream out = openFileOutputStream(emoLogFile);
            redirectors.add(new StdoutRedirector(new InputStreamReader(process.getInputStream()), out));
            redirectors.add(new StdoutRedirector(new InputStreamReader(process.getErrorStream()), out));
        }
        // start all
        for (StdoutRedirector redirector : redirectors) {
            redirector.start();
        }
    }

    private static FileOutputStream openFileOutputStream(File file) {
        final FileOutputStream out;
        try {
            out = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("couldn't create or open file '" + file + "'", e);
        }
        return out;
    }

    public void destroy() {
        for (StdoutRedirector redirector : redirectors) {
            redirector.stopIt();
        }
        process.destroy();
    }

    public void waitFor() {
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}

