Compaction
==========

Compaction is the process of deleting and/or rewriting deltas in the data store
in a more compact format.

Invariants
----------

Compaction must preserve the following invariants:

1.  The "version" field of the object.  Normally, "version" counts the number
    of deltas, so if compaction deletes a delta it must record the number of
    deltas deleted in order to be able to calculate "version" correctly.

2.  The "firstUpdateAt" and "lastUpdateAt" fields.  If compaction deletes a
    delta it must maintain a min and max of the TimeUUID of the deleted deltas
    so that "firstUpdateAt" and "lastUpdateAt" can be computed correctly.

3.  A delta that is not initially considered "redundant" must not become
    redundant as a side effect of compaction.  It is allowed, although not
    particularly desirable, for a redunant delta to become non-redundant
    as a side of compaction.  Note that redundant deltas are a specific
    concern of the DataStore.getForChange() method that uses them to avoid
    sending irrelevant messages on the databus.
    
4.  Deltas are always immutable. Compaction may delete them and retain their
    resolved content, but should not modify any deltas. Note that this is 
    different from the "old" way of compactions which would actually modify 
    the cutoff delta with the resolved literal delta, and compaction record 
    would simply point to them. The DefaultCompactor tackles the old compactions
    that we may still see, but any new compaction it produces makes sure that the 
    cutoff delta remains untouched.

Inputs
------

The general form of the compaction function looks like this:

    compact (delta ..., compaction ..., full consistency delay)
            ->
            (delta' ..., compaction' ...)

A "compaction" record stores non-delta information used to maintain the
invariants above.

Intuitively, compaction will delete deltas.  Less obvious is the
fact that, due to race conditions, we allow multiple compaction records to be
created for the same piece of content.  Those compaction records themselves
can be compacted.

We can assume that all data older than the "full consistency delay" are fully
visible to all readers, and is fully consistent in all data centers.  
This means that a process is allowed to read the data, summarize it, and 
write the result with full confidence that no late-arriving
record will invalidate the summary.  As a result, compaction can be more
efficient on older data than newer data that is still becoming eventually
consistent.


Algorithm
---------

What can be compacted?

1.  Multiple deltas that are older than the "full consistency delay" can be
    resolved together and saved as single constant delta.

    *   The new delta is saved with the TimeUUID of the newest of the
        compacted deltas so that, if multiple compactors race, the one that
        sees the newest content writes the newest delta and wins.

    *   Otherwise, old deltas are deleted.

    *   A compaction record is created that stores the TimeUUID of the
        oldest and newest compacted deltas and the number of compacted
        deltas.
    
    *   A compaction record includes the "compactedDelta" that resolves 
        the content upto the cutoff Id.

2.  Multiple compaction records that compact only deltas older than the "full
    consistency delay" can be resolved together and saved as a single
    compaction record.

    *   Delete all compactible deltas and preserve the resolved literal in the 
        compaction. 

3.  Any deltas that preceed a "constant" delta may be deleted.

    *   A compaction record must be written that preserves all information
        necessary to maintain the invariants above.  TBD.
