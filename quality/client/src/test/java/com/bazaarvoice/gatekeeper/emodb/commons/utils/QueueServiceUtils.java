package com.bazaarvoice.gatekeeper.emodb.commons.utils;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;


public class QueueServiceUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueServiceUtils.class);

    public static List<Object> getMessageList(String client, String runID, String apiTested, List<Object> extras, int size) {
        if (extras != null && extras.size() % 2 != 0) {
            throw new IllegalArgumentException("extras should contain even number of elements");
        }
        List<Object> messagesList = new ArrayList<>();
        IntStream.range(0, size).forEach(i -> {
            List<Object> newExtras = new ArrayList<>();
            if (extras != null) {
                newExtras.addAll(extras);
            }
            newExtras.addAll(Arrays.asList("iteration", i));
            LOGGER.debug("extras: {}", newExtras);

            messagesList.add(getMessage(client, runID, apiTested, newExtras));
        });
        LOGGER.debug("messageList: {}", messagesList);
        return messagesList;
    }

    public static Map<Object, Object> getMessage(String client, String runID, String apiTested, List<Object> extras) {
        ImmutableMap.Builder<Object, Object> returnMapBuilder =
                ImmutableMap.builder()
                        .put("client", client)
                        .put("runID", runID)
                        .put("api", apiTested);

        if (extras != null) {
            IntStream.iterate(0, i -> i + 2).limit(extras.size() / 2).forEach(i -> returnMapBuilder.put(extras.get(i), extras.get(i + 1)));
        }

        return returnMapBuilder.build();
    }
}
