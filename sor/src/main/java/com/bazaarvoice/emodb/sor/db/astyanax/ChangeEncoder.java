package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

interface ChangeEncoder {

    String encodeDelta(String delta, Set<String> tags);

    String encodeAudit(Audit audit);

    String encodeCompaction(Compaction compaction);

    String encodeHistory(History history);

    Change decodeChange(UUID changeId, ByteBuffer buf);

    Compaction decodeCompaction(ByteBuffer buf);
}
