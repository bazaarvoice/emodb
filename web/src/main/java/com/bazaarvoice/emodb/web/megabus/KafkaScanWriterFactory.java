package com.bazaarvoice.emodb.web.megabus;

import com.bazaarvoice.emodb.web.scanner.writer.KafkaScanWriter;
import java.net.URI;

public interface KafkaScanWriterFactory {

    KafkaScanWriter createKafkaScanWriter(URI destination);

}
