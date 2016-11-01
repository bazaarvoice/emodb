package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.db.Record;
import com.google.common.base.Supplier;

import java.util.Iterator;

public interface Compactor {

    Expanded expand(Record record, long fullConsistencyTimestamp, long compactionConsistencyTimeStamp, long compactionControlTimestamp, MutableIntrinsics intrinsics, boolean ignoreRecent,
                    Supplier<Record> requeryFn);

    Iterator<DataAudit> getAuditedContent(PendingCompaction pendingCompaction,
                                         MutableIntrinsics intrinsics);
}
