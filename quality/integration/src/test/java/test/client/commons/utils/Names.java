package test.client.commons.utils;


public class Names {

    public static String uniqueName(String base, String apiTested, String clientName, String runID) {
        String tableName;
        tableName = clientName != null ? clientName.toLowerCase() + ":" : "gatekeeper:";
        tableName += apiTested + "_";
        tableName += base != null ? base + "_" : "";
        tableName += runID;
        return tableName.length() > 256 ? tableName.substring(0, 256) : tableName;
    }
}
