package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.web.scanner.control.ScanRangeComplete;
import com.bazaarvoice.emodb.web.scanner.control.ScanRangeTask;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.List;

public class MigratorWorkflow implements ScanWorkflow {
    @Override
    public void scanStatusUpdated(String scanId) {

    }

    @Override
    public ScanRangeTask addScanRangeTask(String scanId, int taskId, String placement, ScanRange range) {
        return null;
    }

    @Override
    public List<ScanRangeTask> claimScanRangeTasks(int max, Duration ttl) {
        return null;
    }

    @Override
    public void renewScanRangeTasks(Collection<ScanRangeTask> tasks, Duration ttl) {

    }

    @Override
    public void releaseScanRangeTask(ScanRangeTask task) {

    }

    @Override
    public List<ScanRangeComplete> claimCompleteScanRanges(Duration ttl) {
        return null;
    }

    @Override
    public void releaseCompleteScanRanges(Collection<ScanRangeComplete> completions) {

    }
}
