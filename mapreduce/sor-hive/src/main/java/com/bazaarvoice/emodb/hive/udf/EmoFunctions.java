package com.bazaarvoice.emodb.hive.udf;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Class which maintains all available Emo Hive functions.
 */
public class EmoFunctions {

    public static final BiMap<String, Class<? extends UDF>> ALL_FUNCTIONS =
            ImmutableBiMap.<String, Class<? extends UDF>>builder()
                    .put("emo_id", EmoId.class)
                    .put("emo_table", EmoTable.class)
                    .put("emo_coordinate", EmoCoordinate.class)
                    .put("emo_first_update_at", EmoFirstUpdateAt.class)
                    .put("emo_last_update_at", EmoLastUpdateAt.class)
                    .put("emo_last_mutate_at", EmoLastMutateAt.class)
                    .put("emo_text", EmoText.class)
                    .put("emo_boolean", EmoBoolean.class)
                    .put("emo_int", EmoInt.class)
                    .put("emo_long", EmoLong.class)
                    .put("emo_float", EmoFloat.class)
                    .put("emo_timestamp", EmoTimestamp.class)
                    .build();
}
