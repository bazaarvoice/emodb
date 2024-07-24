package com.bazaarvoice.emodb.blob.config;

import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.DefaultBlobMetadata;
import com.bazaarvoice.emodb.blob.api.TenantRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ApiClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiClient.class);
    private final String BASE_URL = "http://localhost:8082/blob";
    private final String TENANT_NAME = "datastorage";
    public final String SUCCESS_MSG = "Successfully deleted blob.";

    public Iterator<BlobMetadata> getBlobMetadata(String tableName) {
        try {
            LOGGER.debug("  Constructing URL and consuming datastorage-media-service URL  ");
            String[] parts = tableName.split(":");
            String table = parts[0];
            String clientName = parts[1];

            // Constructing URL with path variable and query parameters.
            String urlString = String.format("%s/%s/%s/%s",
                    BASE_URL,
                    URLEncoder.encode(TENANT_NAME, "UTF-8"),
                    URLEncoder.encode(table, "UTF-8"),
                    URLEncoder.encode(clientName, "UTF-8"));

            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            // Setting headers
            connection.setRequestProperty("Accept", "application/json");

            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                System.out.println(response);
                LOGGER.info(" Before mapping of the response ");
                return mapResponseToBlobMetaData(response.toString()).iterator();
            } else {
                LOGGER.debug(" GET operation halted with error ");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public String deleteBlobFromTable(String tableName, String blobId) {
        try {
            LOGGER.debug("  Constructing URL and consuming datastorage-media-service delete blob URL  ");
            String[] parts = tableName.split(":");
            String table = parts[0];
            String clientName = parts[1];
            TenantRequest tenantRequest = new TenantRequest(TENANT_NAME);
            System.out.println(" Tenant Request " + tenantRequest);

            // Constructing URL with path variable and query parameters.
            String urlString = String.format("%s/%s:%s/%s",
                    BASE_URL + "/delete",
                    URLEncoder.encode(table, "UTF-8"),
                    URLEncoder.encode(clientName, "UTF-8"),
                    URLEncoder.encode(blobId, "UTF-8"));

            LOGGER.info(" URL {} ", urlString);
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("DELETE");

            // Setting headers
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "application/json");

            // Enable output for the request body
            connection.setDoOutput(true);

            // Write the request body
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = tenantRequest.toString().getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                LOGGER.debug(" Blob with id {} deleted successfully", blobId);
                return SUCCESS_MSG;
            } else {
                LOGGER.debug(" Blob with id {} didn't get deleted ", blobId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public List<BlobMetadata> mapResponseToBlobMetaData(String response) {

        // Parse JSON string to JsonArray
        JsonReader jsonReader = Json.createReader(new StringReader(response));
        JsonArray jsonArray = jsonReader.readArray();
        jsonReader.close();

        // Convert JsonArray to List<POJO>
        List<BlobMetadata> blobMetadata = new ArrayList<>();
        for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
            long length = Long.parseLong(String.valueOf(jsonObject.getInt("length")));

            System.out.println(" Length " + length);
            Map<String, String> attributes = convertStringAttributesToMap((JsonObject) jsonObject.get("attributes"));
            BlobMetadata blobMetadataObject = new DefaultBlobMetadata(jsonObject.getString("id"),
                    convertToDate(jsonObject.getString("timestamp")),
                    length,
                    jsonObject.getString("md5"),
                    jsonObject.getString("sha1"),
                    attributes);
            blobMetadata.add(blobMetadataObject);
            System.out.println(jsonObject);
        }
        LOGGER.info(" After mapping of the response ");
        System.out.println(" BlobMetaData " + blobMetadata);
        return blobMetadata;
    }

    public Date convertToDate(String timestamp) {
        LOGGER.info(" Date to be parsed {} ", timestamp);
        SimpleDateFormat formatter = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);
        try {
            // Parse the string into a Date object
            return formatter.parse(timestamp);
        } catch (ParseException e) {
            LOGGER.error(" Date could not be parsed {} ", timestamp);
        }
        return null;
    }

    public Map<String, String> convertStringAttributesToMap(JsonObject attributes) {
        LOGGER.info(" Attributes to be parsed {} ", attributes);
        // Convert JsonObject to Map<String, String>
        Map<String, String> attributesMap = new HashMap<>();
        for (Map.Entry<String, JsonValue> entry : attributes.entrySet()) {
            String key = entry.getKey();
            JsonValue value = entry.getValue();
            String stringValue;

            // Determine the type of the value and convert accordingly
            switch (value.getValueType()) {
                case STRING:
                    stringValue = ((JsonString) value).getString();
                    break;
                // Handles integers and floats
                case TRUE:
                    stringValue = "true";
                    break;
                case FALSE:
                    stringValue = "false";
                    break;
                case NULL:
                    stringValue = null;
                    break;
                // Convert JSON object/array to string
                default:
                    stringValue = value.toString(); // Fallback for any other types
                    break;
            }
            attributesMap.put(key, stringValue);
        }

        return attributesMap;
    }
}
