package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.megabus.MegabusRef;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import static com.google.common.base.Preconditions.checkNotNull;

public class MissingRefCollection {

    private final List<MegabusRef> _missingRefs;
    private final Date _lastProcessTime;

    @JsonCreator
    public MissingRefCollection(@JsonProperty("missingRefs") List<MegabusRef> missingRefs, @JsonProperty("lastProcessTime") Date lastProcessTime) {
        _missingRefs = checkNotNull(missingRefs);
        _lastProcessTime = checkNotNull(lastProcessTime);
    }

    public List<MegabusRef> getMissingRefs() {
        return _missingRefs;
    }

    public Date getLastProcessTime() {
        return _lastProcessTime;
    }
}
