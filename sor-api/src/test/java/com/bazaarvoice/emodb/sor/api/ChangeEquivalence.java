package com.bazaarvoice.emodb.sor.api;

import com.google.common.base.Equivalence;
import com.google.common.base.Objects;

/**
 * Wrap a {@link Change} instance and provides <tt>equals</tt> and <tt>hashCode</tt> implementations
 * since <tt>Change</tt> doesn't implement them directly.
 */
class ChangeEquivalence extends Equivalence<Change> {
    public static final Equivalence<Change> INSTANCE = new ChangeEquivalence();

    @Override
    protected boolean doEquivalent(Change a, Change b) {
        return Objects.equal(a.getDelta(), b.getDelta()) &&
                Objects.equal(a.getAudit(), b.getAudit()) &&
                CompactionEquivalence.INSTANCE.equivalent(a.getCompaction(), b.getCompaction());
    }

    @Override
    protected int doHash(Change change) {
        return Objects.hashCode(
                change.getDelta(),
                change.getAudit(),
                CompactionEquivalence.INSTANCE.hash(change.getCompaction()));
    }
}
