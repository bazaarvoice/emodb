package com.bazaarvoice.emodb.sor.log;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.dropwizard.logging.ConsoleAppenderFactory;
import io.dropwizard.logging.FileAppenderFactory;
import io.dropwizard.logging.SyslogAppenderFactory;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Factory for {@link LogbackSlowQueryLog} that configures Logback appenders based on a {@link SlowQueryLogConfiguration}.
 */
public class LogbackSlowQueryLogProvider implements Provider<SlowQueryLog> {
    private final SlowQueryLogConfiguration _config;
    private final MetricRegistry _metricRegistry;

    private static final String EMODB_APPLICATION_NAME = "emodb";

    @Inject
    public LogbackSlowQueryLogProvider(SlowQueryLogConfiguration config, MetricRegistry metricRegistry) {
        _config = requireNonNull(config, "config");
        _metricRegistry = metricRegistry;
    }

    @Override
    public SlowQueryLog get() {
        Logger logger = (Logger) LoggerFactory.getLogger("sor.slow-query");
        logger.setAdditive(false);
        LoggerContext context = logger.getLoggerContext();
        logger.detachAndStopAllAppenders();

        PatternLayout logPatternLayout = new PatternLayout();
        logPatternLayout.setPattern("%-5p [%d{ISO8601," + _config.getTimeZone().getID() + "}] %m\n");
        logPatternLayout.setContext(context);
        logPatternLayout.start();

        ConsoleAppenderFactory consoleAppenderFactory = _config.getConsoleAppenderFactory();
        if (consoleAppenderFactory != null) {
            // Console is usually used only in development.  Use a synchronous appender so console output doesn't get re-ordered.
            logger.addAppender(consoleAppenderFactory.build(context, EMODB_APPLICATION_NAME, logPatternLayout));
        }

        FileAppenderFactory fileAppenderFactory = _config.getFileAppenderFactory();
        if (fileAppenderFactory != null) {
            AsyncAppender fileAsyncAppender = new AsyncAppender();
            fileAsyncAppender.addAppender(fileAppenderFactory.build(context, EMODB_APPLICATION_NAME, logPatternLayout));
            fileAsyncAppender.start();
            logger.addAppender(fileAsyncAppender);
        }

        SyslogAppenderFactory syslogAppenderFactory = _config.getSyslogAppenderFactory();
        if (syslogAppenderFactory != null) {
            AsyncAppender sysAsyncAppender = new AsyncAppender();
            sysAsyncAppender.addAppender(syslogAppenderFactory.build(context, EMODB_APPLICATION_NAME, logPatternLayout));
            sysAsyncAppender.start();
            logger.addAppender(sysAsyncAppender);
        }

        return new LogbackSlowQueryLog(logger, _config.getTooManyDeltasThreshold(), _metricRegistry);
    }
}
