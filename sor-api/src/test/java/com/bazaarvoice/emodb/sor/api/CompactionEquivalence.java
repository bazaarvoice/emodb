package com.bazaarvoice.emodb.sor.api;

import com.google.common.base.Equivalence;
import com.google.common.base.Objects;

/**
 * Wrap a {@link Compaction} instance and provide
 * <tt>equals</tt> and <tt>hashCode</tt> implementations since <tt>Compaction</tt> doesn't
 * implement them directly.
 */
class CompactionEquivalence extends Equivalence<Compaction> {
    public static final Equivalence<Compaction> INSTANCE = new CompactionEquivalence();

    @Override
    protected boolean doEquivalent(Compaction a, Compaction b) {
        return a.getCount() == b.getCount() &&
                Objects.equal(a.getFirst(), b.getFirst()) &&
                Objects.equal(a.getCutoff(), b.getCutoff());
    }

    @Override
    protected int doHash(Compaction compaction) {
        return Objects.hashCode(compaction.getCount(), compaction.getFirst());
    }
}
