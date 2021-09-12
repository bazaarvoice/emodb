package test.client.core;

import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.DefaultTable;
import com.bazaarvoice.emodb.blob.api.RangeSpecifications;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;
import test.client.commons.TestModuleFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static test.client.commons.utils.Names.uniqueName;
import static test.client.commons.utils.TableUtils.getAudit;

@Test(timeOut = 360000)
@Guice(moduleFactory = TestModuleFactory.class)
public class BlobStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobStoreTest.class);

    private static final String BLOB_STORE_CLIENT_NAME = "gatekeeper_blobstore_client";
    private static final String DEFAULT_PHOTO_BLOB_NAME = "mandelbrot_color.jpg";

    private static final String MEDIA_DIRECTORY = "media/";

    private Set<String> tablesToCleanupAfterTest;

    @Inject
    private BlobStore blobStore;

    @Inject
    @Named("apiKey")
    private String apiKey;

    @Inject
    @Named("mediaPlacement")
    private String mediaPlacement;

    @Inject
    @Named("emodbHost")
    private String emodbHost;

    @Inject
    @Named("runID")
    private String runID;

    @BeforeTest(alwaysRun = true)
    public void beforeTest() {
        tablesToCleanupAfterTest = new HashSet<>();
    }

    @AfterTest(alwaysRun = true)
    public void afterTest() {
        tablesToCleanupAfterTest.forEach((name) -> {
            LOGGER.info("Deleting blob table: {}", name);
            try {
                blobStore.dropTable(name, getAudit("drop table " + name));
            } catch (Exception e) {
                LOGGER.warn("Error to delete blob table", e);
            }
        });
    }

    @DataProvider
    public Object[][] dataprovider_contentType() {
        Object[][] returnObject;
        try {
            final String pathname = System.getProperty("resources") + MEDIA_DIRECTORY + "mimeTypes.txt";
            List<String> mimeTypeFile = Files.readLines(new File(pathname), StandardCharsets.UTF_8);
            returnObject = new Object[mimeTypeFile.size()][2];
            for (int i = 0; i < mimeTypeFile.size(); i++) {
                String[] lineSplit = mimeTypeFile.get(i).split(",");
                returnObject[i][0] = lineSplit[0].replaceAll("\\s+", "");
                returnObject[i][1] = lineSplit[1].replaceAll("\\s+", "");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return returnObject;
    }

    private static Map<String, Object> getDataProviderData(String resourcePath, String blobName, String contentType, int width, int height) {
        return new HashMap<String, Object>() {
            {
                put("blobName", blobName);
                put("resourcePath", resourcePath);
                put("attr", getBlobAttributes(contentType, width, height));
            }
        };
    }

    private List<Map<String, Object>> geImageList() {
        return new ArrayList<Map<String, Object>>() {{
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish01.jpg", "fil-fish01.jpg", "image/jpg", 800, 540));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish02.jpg", "fil-fish02.jpg", "image/jpg", 809, 801));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish03.jpg", "fil-fish03.jpg", "image/jpg", 800, 533));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish04.jpg", "fil-fish04.jpg", "image/jpg", 800, 600));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish05.jpg", "fil-fish05.jpg", "image/jpg", 720, 560));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish06.jpg", "fil-fish06.jpg", "image/jpg", 800, 600));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish07.jpg", "fil-fish07.jpg", "image/jpg", 800, 600));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish08.jpg", "fil-fish08.jpg", "image/jpg", 800, 533));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish09.jpg", "fil-fish09.jpg", "image/jpg", 600, 800));
            add(getDataProviderData(MEDIA_DIRECTORY + "Fish10.jpg", "fil-fish10.jpg", "image/jpg", 800, 600));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo1.jpg", "pl-photo1.jpg", "image/jpg", 48, 48));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo1.png", "pl-photo1.png", "image/png", 48, 48));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo2.jpg", "pl-photo2.jpg", "image/jpg", 100, 600));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo2.png", "pl-photo2.png", "image/png", 100, 600));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo3.jpg", "pl-photo3.jpg", "image/jpg", 600, 100));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo3.png", "pl-photo3.png", "image/png", 600, 100));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo4.jpg", "pl-photo4.jpg", "image/jpg", 818, 562));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo4.png", "pl-photo4.png", "image/png", 818, 562));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo5.jpg", "pl-photo5.jpg", "image/jpg", 600, 600));
            add(getDataProviderData(MEDIA_DIRECTORY + "photo5.png", "pl-photo5.png", "image/png", 600, 600));
        }};
    }

    @Test
    public void testTableIsEmptyOnCreation() {
        final String tableName = uniqueName("blob", "table_size", BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        assertEquals(blobStore.getTableApproximateSize(tableName), 0);
    }

    @Test
    public void testListTables() {
        int tableCount = Iterators.size(blobStore.listTables(null, Long.MAX_VALUE));
        final String tableName = uniqueName("blob", "list_table", BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        int newTableCount = Iterators.size(blobStore.listTables(null, Long.MAX_VALUE));

        //Could be non-deterministic for live blob cluster,
        // if another blob consumers create/drop tables during test execution
        assertTrue(newTableCount - tableCount >= 1);
    }

    @Test
    public void testTablePlacements() {
        final String tableName = uniqueName("blob", "table_placement", BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        assertTrue(blobStore.getTablePlacements().contains(mediaPlacement));
    }

    @Test
    public void testUploadPhoto() {
        final String tableName = uniqueName("blob", "put", BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        Map<String, String> blobAttributes = getBlobAttributes("image/jpg", 3751, 2813);
        putAndVerifyBlob(blobStore, tableName, DEFAULT_PHOTO_BLOB_NAME, MEDIA_DIRECTORY + DEFAULT_PHOTO_BLOB_NAME, blobAttributes);
    }

    @Test(dataProvider = "dataprovider_contentType")
    public void testContentType(String mimeType, String expectedMimeType) {
        final String s = mimeType
                .replaceAll("/", "_")
                .replaceAll("\\(", "_")
                .replaceAll("\\)", "_");

        final String tableName = uniqueName("content_type", "put_"+ s, BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        Map<String, String> blobAttributes = getBlobAttributes(mimeType, 48, 48);
        putBlob(blobStore, tableName, DEFAULT_PHOTO_BLOB_NAME, getBlobSupplier(MEDIA_DIRECTORY + DEFAULT_PHOTO_BLOB_NAME), blobAttributes);

        String blobUrlString = emodbHost + "/blob/1/" + tableName + "/" + DEFAULT_PHOTO_BLOB_NAME;
        LOGGER.debug("blobUrlString: {}", blobUrlString);

        try {
            URL blobUrl = new URL(blobUrlString);
            HttpURLConnection blobConnection = (HttpURLConnection) blobUrl.openConnection();
            blobConnection.setRequestProperty("X-BV-API-Key", apiKey);
            //get all headers
            Map<String, List<String>> map = blobConnection.getHeaderFields();
            LOGGER.debug("map: {}", map);
            assertEquals(map.get("Content-Type").get(0), expectedMimeType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeletePhoto() {
        final String tableName = uniqueName("blob", "delete", BLOB_STORE_CLIENT_NAME, runID);
        final String photoToDelete = "Fish01.jpg";
        final String anotherPhoto = "Fish02.jpg";
        createDefaultTableAndVerify(tableName);

        putAndVerifyBlob(blobStore, tableName, photoToDelete, MEDIA_DIRECTORY + photoToDelete,
                getBlobAttributes("image/jpg", 800, 540));
        putAndVerifyBlob(blobStore, tableName, anotherPhoto, MEDIA_DIRECTORY + anotherPhoto,
                getBlobAttributes("image/jpg", 809, 801));

        assertEquals(blobStore.getTableApproximateSize(tableName), 2);

        blobStore.delete(tableName, photoToDelete);
        assertEquals(blobStore.getTableApproximateSize(tableName), 1);

        try {
            blobStore.getMetadata(tableName, photoToDelete);
            fail("Metadata should not return for deleted photo");
        } catch (BlobNotFoundException ex) {
            // NOOP
        }
    }

    @Test
    public void testDropTable() {
        final String tableName = uniqueName("blob", "drop_table", BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        putAndVerifyBlob(blobStore, tableName, DEFAULT_PHOTO_BLOB_NAME, MEDIA_DIRECTORY + DEFAULT_PHOTO_BLOB_NAME,
                getBlobAttributes("image/jpg", 2715, 2813));

        assertEquals(blobStore.getTableApproximateSize(tableName), 1);

        blobStore.dropTable(tableName, getAudit("drop table" + tableName));
        assertFalse(blobStore.getTableExists(tableName));
    }

    @Test
    public void testUploadPhotosConcurrently() throws InterruptedException, ExecutionException {
        final String tableName = uniqueName("concurrent", "put", BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        List<Map<String, Object>> fishImageList = geImageList();
        Set<String> expectIds = fishImageList.stream().map(map -> (String) map.get("blobName")).collect(Collectors.toSet());
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        List<Callable<Void>> callables = new ArrayList<>();
        fishImageList.forEach(item -> callables.add(() -> {
            putAndVerifyBlob(blobStore, tableName, (String) item.get("blobName"), (String) item.get("resourcePath"), (Map<String, String>) item.get("attr"));
            return null;
        }));

        List<Future<Void>> futures = executorService.invokeAll(callables);

        for (Future<Void> future : futures) {
            future.get();
        }

        assertEquals(blobStore.getTableApproximateSize(tableName), fishImageList.size());
        Set<String> returnedIDs = new HashSet<>();
        blobStore.scanMetadata(tableName, null, 100).forEachRemaining(blobMetadata -> returnedIDs.add(blobMetadata.getId()));
        assertEquals(returnedIDs, expectIds);
    }

    @Test
    public void testOverwritePhoto() {
        final String tableName = uniqueName("overwrite_photo", "put", BLOB_STORE_CLIENT_NAME, runID);
        final String photoName = "overwrite_photo.jpg";

        createDefaultTableAndVerify(tableName);

        Map<String, Object> firstPhotoMap = geImageList().get(0);
        Map<String, Object> secondPhotoMap = geImageList().get(1);

        putAndVerifyBlob(blobStore, tableName, photoName, (String) firstPhotoMap.get("resourcePath"), (Map<String, String>) firstPhotoMap.get("attr"));
        assertEquals(blobStore.getTableApproximateSize(tableName), 1);

        putAndVerifyBlob(blobStore, tableName, photoName, (String) secondPhotoMap.get("resourcePath"), (Map<String, String>) secondPhotoMap.get("attr"));
        assertEquals(blobStore.getTableApproximateSize(tableName), 1);
    }

    @Test
    public void testUploadVideo() {
        final String tableName = uniqueName("video", "put", BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        List<String> videoList = Arrays.asList("12-cmsStaging.mp4", "18-configureFbAppOptional.mp4");
        Map<String, String> videoAttribute = getBlobAttributes("video/mp4", 1280, 720);

        videoList.forEach(video -> putAndVerifyBlob(blobStore, tableName, video, MEDIA_DIRECTORY + video, videoAttribute));
    }

    @Test
    public void testRangeSpec() {
        final String tableName = uniqueName("photo", "range", BLOB_STORE_CLIENT_NAME, runID);
        createDefaultTableAndVerify(tableName);

        Map<String, String> blobAttributes = getBlobAttributes("image/jpg", 3751, 2813);
        putAndVerifyBlob(blobStore, tableName, DEFAULT_PHOTO_BLOB_NAME, MEDIA_DIRECTORY + DEFAULT_PHOTO_BLOB_NAME, blobAttributes);

        byte[] expectedBlobByteArray = getResourceByteArray(MEDIA_DIRECTORY + DEFAULT_PHOTO_BLOB_NAME);

        final String blobVerifyFile = "/tmp/blob_verify_" + DEFAULT_PHOTO_BLOB_NAME;
        long offset = expectedBlobByteArray.length / 4;
        OutputStream outputStream;
        Blob blob;
        File downloadedBlob;
        URL downloadedBlobURL;
        int downloadCounter = 0;
        try {
            do {
                outputStream = new FileOutputStream(blobVerifyFile);
                for (int i = 0; i < 4; i++) {
                    blob = blobStore.get(tableName, DEFAULT_PHOTO_BLOB_NAME, RangeSpecifications.slice(i * offset, offset));
                    blob.writeTo(outputStream);

                    if (i == 3) {
                        outputStream.flush();
                        outputStream.close();
                    }
                }
                downloadedBlob = new File(blobVerifyFile);
                downloadCounter++;
            } while (!downloadedBlob.exists() && downloadCounter <= 3);
            assertNotNull(downloadedBlob, "Failed to download photo");
            assertTrue(downloadedBlob.exists(), "Downloaded photo file does not exists");
            downloadedBlobURL = downloadedBlob.toURI().toURL();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] actualBlobByteArray = getFileByteArray(downloadedBlobURL);
        assertEquals(actualBlobByteArray, expectedBlobByteArray);
    }

    private void createBlobTable(String tableName, TableOptions options, Map<String, String> attributes) {
        LOGGER.debug("Creating table {}", tableName);
        LOGGER.debug("Placement {}", options.getPlacement());

        try {
            blobStore.createTable(tableName, options, attributes, getAudit(String.format("Creating table: %s", tableName)));
        } catch (TableExistsException te) {
            tablesToCleanupAfterTest.add(tableName);
            throw new com.bazaarvoice.emodb.sor.api.TableExistsException(String.format("Cannot create table that already exists: %s", tableName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        tablesToCleanupAfterTest.add(tableName);
        LOGGER.debug("Created table {}", tableName);
    }

    private void createDefaultTableAndVerify(String tableName) {
        final TableOptions options = new TableOptionsBuilder().setPlacement(mediaPlacement).build();
        final Map<String, String> tableAttributes = ImmutableMap.of("type", "photo", "client", BLOB_STORE_CLIENT_NAME);

        createBlobTable(tableName, options, tableAttributes);
        assertTrue(blobStore.getTableExists(tableName), String.format("Table '%s' was not created!", tableName));
        assertEquals(blobStore.getTableAttributes(tableName), tableAttributes);
        assertEquals(blobStore.getTableOptions(tableName), options);
        assertEquals(blobStore.getTableMetadata(tableName), new DefaultTable(tableName, options, tableAttributes, null));
    }

    private static Map<String, String> getBlobAttributes(String contentType, int width, int height) {
        return ImmutableMap.of(
                "width", Integer.toString(width),
                "height", Integer.toString(height),
                "contentType", contentType);
    }

    private void putAndVerifyBlob(BlobStore blobStore, String tableName, String blobName, String resourcePath, Map<String, String> blobAttributes) {
        putBlob(blobStore, tableName, blobName, getBlobSupplier(resourcePath), blobAttributes);
        verifyBlob(tableName, blobName, resourcePath, blobAttributes);
    }

    private static Supplier<InputStream> getBlobSupplier(String resourcePath) {
        return () -> new ByteArrayInputStream(getResourceByteArray(resourcePath));
    }

    private void verifyBlob(String tableName, String blobName, String resourcePath, Map<String, String> expectedBlobAttributes) {
        Map<String, String> actualBlobAttribute = blobStore.getMetadata(tableName, blobName).getAttributes();
        expectedBlobAttributes.forEach((key, value) -> {
            assertTrue(actualBlobAttribute.containsKey(key), String.format("'%s' key not found in actual", key));
            assertEquals(actualBlobAttribute.get(key), expectedBlobAttributes.get(key),
                    String.format("Value for key '%s' not the same", key));
        });

        final String blobVerifyFile = "/tmp/blob_verify_" + blobName;
        try {
            File downloadedBlob = downloadBlobToFile(blobStore, tableName, blobName, blobVerifyFile);
            int downloadCounter = 0;
            while (downloadCounter < 3 && !downloadedBlob.exists()) {
                downloadedBlob = downloadBlobToFile(blobStore, tableName, blobName, blobVerifyFile);
                downloadCounter++;
            }
            assertNotNull(downloadedBlob, "Downloaded file is null");
            assertTrue(downloadedBlob.exists(), "Unable to download photo: " + blobName + " from table: " + tableName);
            byte[] actualBlobByteArray = getFileByteArray(downloadedBlob.toURI().toURL());
            byte[] expectedBlobByteArray = getResourceByteArray(resourcePath);
            assertEquals(actualBlobByteArray, expectedBlobByteArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            new File(blobVerifyFile).delete();
        }
    }

    private static void putBlob(BlobStore blobStore, String tableName, String blobName, Supplier<? extends InputStream> inputSupplier, Map<String, String> blobAttributes) {
        LOGGER.debug("starting blob put for {}", blobName);

        LOGGER.debug("TableName: {}", tableName);
        LOGGER.debug("blobName: {}", blobName);
        LOGGER.debug("attr: {}", blobAttributes);

        try {
            blobStore.put(tableName, blobName, inputSupplier, blobAttributes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static File downloadBlobToFile(BlobStore blobStore, String tableName, String blobName, final String blobFile) {
        File downloadedBlob;
        try {
            Blob blob = blobStore.get(tableName, blobName);
            OutputStream outputStream = new FileOutputStream(blobFile);
            blob.writeTo(outputStream);
            outputStream.flush();
            outputStream.close();
            downloadedBlob = new File(blobFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return downloadedBlob;
    }

    private static byte[] getResourceByteArray(String resourcePath) {
        URL resourceUrl = Thread.currentThread().getContextClassLoader().getResource(resourcePath);
        return getFileByteArray(resourceUrl);
    }

    private static byte[] getFileByteArray(URL file) {
        byte[] inputByteArray;

        try {
            inputByteArray = IOUtils.toByteArray(file.openStream());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return inputByteArray;
    }
}
