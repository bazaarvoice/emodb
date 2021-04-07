package test.client.commons.utils;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.google.common.collect.ImmutableMap;

import java.util.Map;


public class TableUtils {
    public static Audit getAudit(String comment) {
        return new AuditBuilder()
                .setProgram("Gatekeeper")
                .setComment(comment)
                .setLocalHost()
                .build();
    }

    public static Map<String, String> getTemplate(String apiTested, String clientName, String runID) {
        return ImmutableMap.of("apiTested", apiTested, "client", clientName, "runID", runID);
    }
}
