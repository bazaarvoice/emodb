
package com.bazaarvoice.emodb.sor.log;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.pattern.PatternLayoutBase;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.dropwizard.logging.ConsoleAppenderFactory;
import io.dropwizard.logging.FileAppenderFactory;
import io.dropwizard.logging.SyslogAppenderFactory;
import io.dropwizard.logging.async.AsyncLoggingEventAppenderFactory;
import io.dropwizard.logging.filter.NullLevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;
import java.util.TimeZone;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Factory for {@link LogbackSlowQueryLog} that configures Logback appenders based on a {@link SlowQueryLogConfiguration}.
 */
public class LogbackSlowQueryLogProvider implements Provider<SlowQueryLog> {
    private final SlowQueryLogConfiguration _config;
    private final MetricRegistry _metricRegistry;

    private static final String EMODB_APPLICATION_NAME = "emodb";

    @Inject
    public LogbackSlowQueryLogProvider(SlowQueryLogConfiguration config, MetricRegistry metricRegistry) {
        _config = checkNotNull(config, "config");
        _metricRegistry = metricRegistry;
    }

    @Override
    public SlowQueryLog get() {
        Logger logger = (Logger) LoggerFactory.getLogger("sor.slow-query");
        logger.setAdditive(false);
        LoggerContext context = logger.getLoggerContext();
        logger.detachAndStopAllAppenders();

        LayoutFactory<ILoggingEvent> layoutFactory = new LayoutFactory<ILoggingEvent>() {
            @Override
            public PatternLayoutBase<ILoggingEvent> build(LoggerContext context, TimeZone timeZone) {
                PatternLayout logPatternLayout = new PatternLayout();
                logPatternLayout.setPattern("%-5p [%d{ISO8601," + _config.getTimeZone().getID() + "}] %m\n");
                logPatternLayout.setContext(context);
                logPatternLayout.start();
                return logPatternLayout;
            }
        };

        ConsoleAppenderFactory consoleAppenderFactory = _config.getConsoleAppenderFactory();
        if (consoleAppenderFactory != null) {
            // Console is usually used only in development.  Use a synchronous appender so console output doesn't get re-ordered.
            logger.addAppender(consoleAppenderFactory.build(context, EMODB_APPLICATION_NAME, layoutFactory, new NullLevelFilterFactory(), new AsyncLoggingEventAppenderFactory()));
        }

        FileAppenderFactory fileAppenderFactory = _config.getFileAppenderFactory();
        if (fileAppenderFactory != null) {
            AsyncAppender fileAsyncAppender = new AsyncAppender();
            fileAsyncAppender.addAppender(fileAppenderFactory.build(context, EMODB_APPLICATION_NAME, layoutFactory, new NullLevelFilterFactory(), new AsyncLoggingEventAppenderFactory()));
            fileAsyncAppender.start();
            logger.addAppender(fileAsyncAppender);
        }

        SyslogAppenderFactory syslogAppenderFactory = _config.getSyslogAppenderFactory();
        if (syslogAppenderFactory != null) {
            AsyncAppender sysAsyncAppender = new AsyncAppender();
            sysAsyncAppender.addAppender(syslogAppenderFactory.build(context, EMODB_APPLICATION_NAME, layoutFactory, new NullLevelFilterFactory(), new AsyncLoggingEventAppenderFactory()));
            sysAsyncAppender.start();
            logger.addAppender(sysAsyncAppender);
        }

        return new LogbackSlowQueryLog(logger, _config.getTooManyDeltasThreshold(), _metricRegistry);
    }
}
