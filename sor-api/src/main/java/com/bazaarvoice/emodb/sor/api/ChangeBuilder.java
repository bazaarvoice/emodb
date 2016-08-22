package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.sor.delta.Delta;

import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Helper for constructing instances of {@link Change}.
 */
public final class ChangeBuilder {
    private final UUID _changeId;
    private Delta _delta;
    private Audit _audit;
    private Compaction _compaction;
    private Set<String> _tags;
    private History _history;

    public static Change merge(Change change1, Change change2) {
        checkArgument(change1 != null || change2 != null);
        if (change1 == null) {
            return change2;
        } else if (change2 == null) {
            return change1;
        } else {
            return new ChangeBuilder(change1.getId())
                    .merge(change1)
                    .merge(change2)
                    .build();
        }
    }

    public ChangeBuilder(UUID changeId) {
        _changeId = changeId;
    }

    public ChangeBuilder merge(Change change) {
        if (change == null) {
            return this;
        }
        checkArgument(_changeId.equals(change.getId()));
        if (change.getDelta() != null) {
            _delta = change.getDelta();
        }
        if (change.getAudit() != null) {
            _audit = change.getAudit();
        }
        if (change.getCompaction() != null) {
            _compaction = change.getCompaction();
        }
        if (change.getHistory() != null) {
            _history = change.getHistory();
        }
        return this;
    }

    public boolean isEmpty() {
        return _delta == null &&
                _audit == null &&
                _compaction == null &&
                _history == null;
    }

    public static Change just(UUID changeId, Delta delta) {
        return new Change(changeId, delta, null, null, null, null);
    }

    public static Change just(UUID changeId, Delta delta, Set<String> tags) {
        return new Change(changeId, delta, null, null, null, tags);
    }

    public ChangeBuilder with(Delta delta) {
        _delta = delta;
        return this;
    }

    public ChangeBuilder with(Set<String> tags) {
        _tags = tags;
        return this;
    }

    public static Change just(UUID changeId, Audit audit) {
        return new Change(changeId, null, audit, null, null, null);
    }

    public ChangeBuilder with(Audit audit) {
        _audit = audit;
        return this;
    }

    public static Change just(UUID changeId, Compaction compaction) {
        return new Change(changeId, null, null, compaction, null, null);
    }

    public ChangeBuilder with(Compaction compaction) {
        _compaction = compaction;
        return this;
    }

    public ChangeBuilder with(History history) {
        _history = history;
        return this;
    }

    public Change build() {
        return new Change(_changeId, _delta, _audit, _compaction, _history, _tags);
    }
}
