package com.bazaarvoice.emodb.web.megabus;

import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.megabus.MegabusBootDAO;
import com.google.inject.Inject;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

public class StashMegabusBootDAO implements MegabusBootDAO {

    private final ScanUploader _scanUploader;
    private final DataTools _dataTools;

    @Inject
    public StashMegabusBootDAO(ScanUploader scanUploader, DataTools dataTools) {
        _scanUploader = checkNotNull(scanUploader);
        _dataTools = checkNotNull(dataTools);
    }

    @Override
    public void initiateBoot(String applicationId, Topic topic) {
        ScanOptions scanOptions = new ScanOptions(_dataTools.getTablePlacements(false, true));
        scanOptions.setTemporalEnabled(false);
        scanOptions.setOnlyScanLiveRanges(false);
        scanOptions.addDestination(ScanDestination.to(URI.create("kafka://" + topic.getName())));

        _scanUploader.scanAndUpload(applicationId, scanOptions).start();
    }

    @Override
    public BootStatus getBootStatus(String applicationId) {
        ScanStatus status = _scanUploader.getStatus(applicationId);

        if (status == null) {
            return BootStatus.NOT_STARTED;
        }

        // Cancel check should go "before" Done check as isDone() returns true even if it's cancelled.
        if (status.isCanceled()) {
            return BootStatus.FAILED;
        }

        if (status.isDone()) {
            return BootStatus.COMPLETE;
        }

        return BootStatus.IN_PROGRESS;
    }
}
