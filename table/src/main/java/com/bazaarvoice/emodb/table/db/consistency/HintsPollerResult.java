package com.bazaarvoice.emodb.table.db.consistency;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class HintsPollerResult {
    private final Map<InetAddress, Optional<Long>> _hintsInfo;
    private final Set<InetAddress> _hostFailure = Sets.newHashSet();

    public HintsPollerResult() {
        _hintsInfo = Maps.newHashMap();
    }

    public HintsPollerResult setHintsResult(InetAddress hostAddress, Optional<Long> oldestHintTime) {
        _hintsInfo.put(hostAddress, oldestHintTime);
        return this;
    }

    public HintsPollerResult setHostWithFailure(InetAddress failedHost) {
        _hostFailure.add(failedHost);
        return this;
    }

    public Set<InetAddress> getAllPolledHosts() {
        return Sets.union(_hintsInfo.keySet(), _hostFailure);
    }

    /**
     * @return the oldest hint's timestamp.
     * Note: Returns an absent Optional if there are no hints found in the entire ring
     * Throws IllegalStateException if there were host failures while polling
     */
    public Optional<Long> getOldestHintTimestamp() {
        checkState(areAllHostsPolling(), "There were host failures while polling");
        Map<InetAddress, Long> hostsWithHints = getHostsWithHints(_hintsInfo);
        if (hostsWithHints.isEmpty()) {
            // No hints found in the ring
            return Optional.empty();
        }
        return Optional.of(Collections.min(hostsWithHints.values()));
    }

    public Set<InetAddress> getHostFailure() {
        return _hostFailure;
    }

    public boolean areAllHostsPolling() {
        return _hostFailure.isEmpty();
    }

    private Map<InetAddress, Long> getHostsWithHints(Map<InetAddress, Optional<Long>> hintsInfo) {
        return Maps.transformEntries(
                Maps.filterEntries(hintsInfo, new Predicate<Map.Entry<InetAddress, Optional<Long>>>() {
                    @Override
                    public boolean apply(Map.Entry<InetAddress, Optional<Long>> input) {
                        return input.getValue().isPresent();
                    }
                }), new Maps.EntryTransformer<InetAddress, Optional<Long>, Long>() {
                    @Override
                    public Long transformEntry(InetAddress key, Optional<Long> value) {
                        return value.get();
                    }
                });
    }

}
