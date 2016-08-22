package com.bazaarvoice.emodb.web.scanner.notifications;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.plugin.PluginServerMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashMetadata;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

/**
 * Scan notifier which publishes scan completion to an SNS topic.
 */
public class SNSStashStateListener implements StashStateListener<Void> {

    private final Logger _log = LoggerFactory.getLogger(SNSStashStateListener.class);

    private final AmazonSNS _amazonSNS;
    private final Supplier<String> _completedTopic;

    public SNSStashStateListener(AmazonSNS amazonSNS, String completedTopic) {
        _amazonSNS = amazonSNS;
        _completedTopic = createTopicSupplier(completedTopic);
    }

    @Override
    public void init(Environment environment, PluginServerMetadata metadata, Void ignore) {
        // no-op
    }

    private Supplier<String> createTopicSupplier(final String topicName) {
        return Suppliers.memoize(new Supplier<String>() {
            @Override
            public String get() {
                // This will create the topic if it doesn't exist or return the existing topic if it does.
                CreateTopicResult topic = _amazonSNS.createTopic(topicName);
                return topic.getTopicArn();
            }
        });
    }

    @Override
    public void stashCompleted(StashMetadata info, Date completeTime) {
        Map<String, Object> message = ImmutableMap.<String, Object>of(
                "id", info.getId(),
                "status", "completed",
                "destinations",
                        FluentIterable.from(info.getDestinations())
                                .transform(Functions.toStringFunction())
                                .toList(),
                "completedTime", completeTime);

        try {
            _amazonSNS.publish(_completedTopic.get(), JsonHelper.asJson(message));
        } catch (Exception e) {
            _log.error("Failed to send SNS notification of scan completed: id={}", info.getId(), e);
        }
    }

    @Override
    public void announceStashParticipation() {
        // No action on stash participation announcement
    }

    @Override
    public void stashStarted(StashMetadata info) {
        // No action on stash started
    }

    @Override
    public void stashCanceled(StashMetadata info, Date canceledTime) {
        // No action on stash canceled
    }
}
