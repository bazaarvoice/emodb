package com.bazaarvoice.emodb.hadoop.json;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

/**
 * Object that can be used to efficiently deserialize just the intrinsics from an EmoDB json literal while ignoring
 * the remaining properties.
 */
@JsonIgnoreProperties (ignoreUnknown = true)
public class IntrinsicsOnly {
    @JsonProperty (Intrinsic.ID)
    public String id;
    @JsonProperty (Intrinsic.TABLE)
    public String table;
    @JsonProperty (Intrinsic.VERSION)
    public long version;
    @JsonProperty (Intrinsic.SIGNATURE)
    public String signature;
    @JsonProperty (Intrinsic.FIRST_UPDATE_AT)
    public Date firstUpdateAt;
    @JsonProperty (Intrinsic.LAST_UPDATE_AT)
    public Date lastUpdateAt;
    @JsonProperty (Intrinsic.LAST_MUTATE_AT)
    public Date lastMutateAt;
}
