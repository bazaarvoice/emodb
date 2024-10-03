package com.bazaarvoice.emodb.blob.config;

import com.bazaarvoice.emodb.blob.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ApiClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiClient.class);
//    private final String BASE_URL = "https://cert-blob-media-service.qa.us-east-1.nexus.bazaarvoice.com/blob";
    private final String BASE_URL = "https://cert-blob-media-service.qa.us-east-1.nexus.bazaarvoice.com/blob";
    private final String TENANT_NAME = "datastorage";
    public final String SUCCESS_MSG = "Successfully deleted blob.";

    public Iterator<BlobMetadata> getBlobMetadata(String fromBlobIdExclusive) {
        try {
            LOGGER.debug("  Constructing URL and consuming datastorage-media-service URL  ");

            // Constructing URL with path variable and query parameters.
            String urlString = String.format("%s/%s/%s",
                    BASE_URL,
                    URLEncoder.encode(fromBlobIdExclusive, "UTF-8"),
                    "/metadata");

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
                LOGGER.info(" Before mapping of the response {} ", response);
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

    public void uploadBlobFromByteArray(String tableName, String blobId, String md5, String sha1, Map<String, String> attributes,
                                        InputStream inputStream) {
        try {
            LOGGER.debug("  Constructing URL and consuming datastorage-media-service upload blob byte array URL  ");
            String[] parts = tableName.split(":");
            String table = parts[0];
            String clientName = parts[1];
            UploadByteRequestBody uploadByteRequestBody = createUploadBlobRequestBody(table, clientName, blobId,
                    md5, sha1, attributes, inputStream);
            // Constructing URL with path variable and query parameters.
            String urlString = String.format("%s/%s:%s/%s?contentType=%s",
                    BASE_URL + "/uploadByteArray",
                    URLEncoder.encode(table, "UTF-8"),
                    URLEncoder.encode(clientName, "UTF-8"),
                    URLEncoder.encode(blobId, "UTF-8"),
                    URLEncoder.encode("image/jpeg", "UTF-8"));

            LOGGER.info(" URL {} ", urlString);
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");

            // Setting headers
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "*/*");
            connection.setRequestProperty("X-BV-API-KEY", "cert_admin");

            // Enable output for the request body
            connection.setDoOutput(true);

            // Write the request body
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = uploadByteRequestBody.toString().getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }


            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                LOGGER.debug(" Blob with id {} uploaded successfully", blobId);
            } else {
                LOGGER.debug(" Blob with id {} didn't get uploaded ", blobId);
            }
        } catch (Exception e) {
            LOGGER.error(" Exception occurred during putting the object to s3 ", e);
        }

    }

    public byte[] getBlob(String tableName, String blobId, Map<String, String> headers) {
        try {
            // Define the path variables
            String[] parts = tableName.split(":");
            String table = parts[0];
            String clientName = parts[1];
            String inputLine;

            // Build the URL for the endpoint
            String endpointUrl = String.format("%s/%s/%s/%s/%s",
                    BASE_URL,
                    URLEncoder.encode(TENANT_NAME, "UTF-8"),
                    URLEncoder.encode(table, "UTF-8"),
                    URLEncoder.encode(clientName, "UTF-8"),
                    URLEncoder.encode(blobId, "UTF-8"));

            // Create a URL object
            URL url = new URL(endpointUrl);

            // Open a connection to the URL
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // Set the request method to GET
            connection.setRequestMethod("GET");

            //Set "Connection" header to "keep-alive"
            connection.setRequestProperty("Connection", "keep-alive");

            // Get the response code
            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            // Check if the response is OK (200)
            if (responseCode == HttpURLConnection.HTTP_OK) {
                Map<String, List<String>> responseHeaders = connection.getHeaderFields();

                // Print each header key and its values
                for (Map.Entry<String, List<String>> entry : responseHeaders.entrySet()) {
                    String headerName = entry.getKey();
                    List<String> headerValues = entry.getValue();

                    System.out.println("Header: " + headerName);
                    for (String value : headerValues) {
                        headers.put(headerName, value);
                        System.out.println("Value: " + value);
                    }
                }
                InputStream inputStream = connection.getInputStream();

                // Read the input stream into a byte array
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int bytesRead;

                // Read the input stream into the buffer and write to ByteArrayOutputStream
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    byteArrayOutputStream.write(buffer, 0, bytesRead);
                }

                // Convert the ByteArrayOutputStream to a byte array
                byte[] responseBytes = byteArrayOutputStream.toByteArray();

                // Optionally, you can do something with the byte array (e.g., save it as a file)
                System.out.println("Response received as byte array, length: " + responseBytes.length);

                // Close the streams
                inputStream.close();
                byteArrayOutputStream.close();
                return responseBytes;
            } else if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
                System.out.println("Blob not found (404)");

            } else if (responseCode == HttpURLConnection.HTTP_INTERNAL_ERROR) {
                System.out.println("Internal server error (500)");

            } else {
                System.out.println("Unexpected response code: " + responseCode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private List<BlobMetadata> mapResponseToBlobMetaData(String response) {

        // Parse JSON string to JsonArray
        JsonReader jsonReader = Json.createReader(new StringReader(response));
        JsonArray jsonArray = jsonReader.readArray();
        jsonReader.close();

        // Convert JsonArray to List<POJO>
        List<BlobMetadata> blobMetadata = new ArrayList<>();
        for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class)) {
            long length = Long.parseLong(String.valueOf(jsonObject.getInt("length")));

            Map<String, String> attributes = convertStringAttributesToMap((JsonObject) jsonObject.get("attributes"));
            BlobMetadata blobMetadataObject = new DefaultBlobMetadata(jsonObject.getString("id"),
                    convertToDate(jsonObject.getString("timestamp")),
                    length,
                    jsonObject.getString("md5"),
                    jsonObject.getString("sha1"),
                    attributes);
            blobMetadata.add(blobMetadataObject);
        }
        LOGGER.debug(" After mapping of the response {} ", blobMetadata);
        return blobMetadata;
    }

    private Date convertToDate(String timestamp) {
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

    private Map<String, String> convertStringAttributesToMap(JsonObject attributes) {
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

    private UploadByteRequestBody createUploadBlobRequestBody(String table, String clientName, String blobId, String md5,
                                                              String sha1, Map<String, String> attributes,
                                                              InputStream inputStream) {
        PlatformClient platformClient = new PlatformClient(table, clientName);
        Attributes attributesForRequest = new Attributes(clientName, "image/jpeg",
                "", platformClient.getTable() + ":" + platformClient.getClientName(), "", "photo");
        BlobAttributes blobAttributesForRequest = new BlobAttributes(blobId, createTimestamp(), 0, md5, sha1, attributesForRequest);
        return new UploadByteRequestBody(convertInputStreamToBase64(inputStream),
                TENANT_NAME, blobAttributesForRequest);
    }

    private String createTimestamp() {
        SimpleDateFormat formatter = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);

        // Set the time zone to GMT
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));

        // Get the current date
        Date currentDate = new Date();

        // Format the current date
        return formatter.format(currentDate);
    }

    private String convertInputStreamToBase64(InputStream inputStream) {
        try {
            // Convert InputStream to Base64 encoded string
            return convertToBase64(inputStream);
        } catch (IOException e) {
            LOGGER.error(" InputStream cannot be converted into base64... ", e);
        }
        return null;
    }

    public String convertToBase64(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int bytesRead;

        // Read bytes from the InputStream and write them to the ByteArrayOutputStream
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }

        // Convert the ByteArrayOutputStream to a byte array
        byte[] byteArray = outputStream.toByteArray();

        // Encode the byte array to a Base64 encoded string
        return Base64.getEncoder().encodeToString(byteArray);
    }
}
