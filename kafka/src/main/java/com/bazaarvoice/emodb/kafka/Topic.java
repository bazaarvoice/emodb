package com.bazaarvoice.emodb.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class Topic {

    private String _name;

    private int _partitions;

    private short _replicationFactor;

    @JsonCreator
    @VisibleForTesting
    public Topic(@Valid @NotNull @JsonProperty("name") String name,
                 @Valid @NotNull @JsonProperty("partitions") int partitions,
                 @Valid @NotNull @JsonProperty("replicationFactor") short replicationFactor) {
        this._name = name;
        this._partitions = partitions;
        this._replicationFactor = replicationFactor;
    }

    private Topic() { }

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
