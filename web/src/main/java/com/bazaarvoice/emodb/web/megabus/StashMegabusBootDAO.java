package com.bazaarvoice.emodb.web.megabus;

import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.bazaarvoice.megabus.MegabusBootDAO;
import com.google.inject.Inject;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

public class StashMegabusBootDAO implements MegabusBootDAO {

    private final ScanUploader _scanUploader;

    @Inject
    public StashMegabusBootDAO(ScanUploader scanUploader) {
        _scanUploader = checkNotNull(scanUploader);
    }

    @Override
    public void initiateBoot(String applicationId, Topic topic) {
        //TODO: this needs all placements
        ScanOptions scanOptions = new ScanOptions("ugc_global:ugc");
        scanOptions.addDestination(ScanDestination.to(URI.create("kafka://" + topic.getName())));

        _scanUploader.scanAndUpload(applicationId, scanOptions).start();
    }

    @Override
    public BootStatus getBootStatus(String applicationId) {
        ScanStatus status = _scanUploader.getStatus(applicationId);

        if (status == null) {
            return BootStatus.NOT_STARTED;
        }

        if (status.isDone()) {
            return BootStatus.COMPLETE;
        }

        if (status.isCanceled()) {
            return BootStatus.FAILED;
        }

        return BootStatus.IN_PROGRESS;
    }
}
