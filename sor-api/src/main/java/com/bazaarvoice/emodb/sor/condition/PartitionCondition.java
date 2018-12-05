package com.bazaarvoice.emodb.sor.condition;

/**
 * Condition to explicitly partition the events based on a hash of the document ID.  The condition takes two parameters:
 *
 * <ol>
 *     <li>The total number of partitions</li>
 *     <li>A condition which tests whether the partition for the current document is a match</li>
 * </ol>
 *
 * For example, the following two conditions should roughly split all matching documents between them:
 *
 * <code>
 *     and({..,"type":"review"},partition(2:1))
 *     and({..,"type":"review"},partition(2:2))
 * </code>
 *
 * Note that partitions are 1-based and so for a number of partitions <code>p</code> the possible matching conditions
 * are the integers in <code>[1..p]</code>.  There are some checks preventing the client from providing invalid
 * conditions but these are non-exhaustive and do not cover meaningless conditions.  For example, the following
 * conditions will result no matches:
 *
 * <code>
 *     partition(4: ge(5))
 *     partition(4: and(1,2))
 * </code>
 */
public interface PartitionCondition extends Condition {

    /**
     * Total number of partitions.  Must be a positive integer.
     */
    int getNumPartitions();

    /**
     * Condition to evaluate whether the partition for the current document matches.  Most commonly this will be
     * a simple equality condition such as <code>eq(1)</code> which can be shortened to just <code>1</code>.
     */
    Condition getCondition();
}
