package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.kafka.Topic;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class MegabusConfiguration {

    @Valid
    @NotNull
    @JsonProperty("applicationId")
    private String _applicationId;

    @Valid
    @NotNull
    @JsonProperty("megabusRefTopic")
    private Topic _megabusRefTopic;

    @Valid
    @NotNull
    @JsonProperty("megabusTopic")
    private Topic _megabusTopic;

    @Valid
    @NotNull
    @JsonProperty("boot")
    private MegabusBootConfiguration _bootConfiguration;

    public String getApplicationId() {
        return _applicationId;
    }

    public Topic getMegabusRefTopic() {
        return _megabusRefTopic;
    }

    public Topic getMegabusTopic() {
        return _megabusTopic;
    }

    public MegabusBootConfiguration getBootConfiguration() {
        return _bootConfiguration;
    }
}
