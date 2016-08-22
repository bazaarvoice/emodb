package com.bazaarvoice.emodb.sor.log;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.logging.ConsoleAppenderFactory;
import io.dropwizard.logging.FileAppenderFactory;
import io.dropwizard.logging.SyslogAppenderFactory;

import javax.validation.constraints.NotNull;
import java.util.TimeZone;

public class SlowQueryLogConfiguration {

    @JsonProperty("console")
    private ConsoleAppenderFactory _console;

    @JsonProperty("file")
    private FileAppenderFactory _file;

    @JsonProperty("syslog")
    private SyslogAppenderFactory _syslog;

    @NotNull
    @JsonProperty("timeZone")
    private TimeZone _timeZone = TimeZone.getTimeZone("UTC");

    @JsonProperty("tooManyDeltasThreshold")
    private int _tooManyDeltasThreshold = 20;

    public ConsoleAppenderFactory getConsoleAppenderFactory() {
        return _console;
    }

    public FileAppenderFactory getFileAppenderFactory() {
        return _file;
    }

    public SyslogAppenderFactory getSyslogAppenderFactory() {
        return _syslog;
    }

    public TimeZone getTimeZone() {
        return _timeZone;
    }

    public int getTooManyDeltasThreshold() {
        return _tooManyDeltasThreshold;
    }
}
