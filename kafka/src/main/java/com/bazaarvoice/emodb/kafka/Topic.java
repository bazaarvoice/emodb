package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class Topic {

    @Valid
    @NotNull
    @JsonProperty("name")
    private String _name;

    @Valid
    @NotNull
    @JsonProperty("partitions")
    private int _partitions;

    @Valid
    @NotNull
    @JsonProperty("replicationFactor")
    private short _replicationFactor;

    @VisibleForTesting
    public Topic(String name, int partitions, short replicationFactor) {
        this._name = name;
        this._partitions = partitions;
        this._replicationFactor = replicationFactor;
    }

    public String getName() {
        return _name;
    }

    public int getPartitions() {
        return _partitions;
    }

    public short getReplicationFactor() {
        return _replicationFactor;
    }
}
