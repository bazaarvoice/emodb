package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

public interface ChangeEncoder {

    String encodeDelta(String delta, @Nullable EnumSet<ChangeFlag> changeFlags, Set<String> tags, StringBuilder changeBody);

    String encodeAudit(Audit audit);

    String encodeCompaction(Compaction compaction, StringBuilder builder);

    String encodeHistory(History history);

    Change decodeChange(UUID changeId, ByteBuffer buf);

    Compaction decodeCompaction(ByteBuffer buf);
}
