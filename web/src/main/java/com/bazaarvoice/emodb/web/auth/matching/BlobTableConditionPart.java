package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.condition.Condition;

import javax.annotation.Nullable;

/**
 * {@link TableConditionPart} implementation for evaluating blob tables.
 */
public class BlobTableConditionPart extends TableConditionPart {

    private final BlobStore _blobStore;

    public BlobTableConditionPart(Condition condition, BlobStore blobStore) {
        super(condition);
        _blobStore = blobStore;
    }

    @Nullable
    @Override
    protected PlacementAndAttributes getPlacementAndAttributesForTable(String tableName, boolean useMasterPlacement) {
        // In some Emo configurations there may legitimately not be a blob store.  If this is the case
        // don't raise an exception, just deny permission.
        if (_blobStore == null) {
            return null;
        }

        Table table =_blobStore.getTableMetadata(tableName);
        return new PlacementAndAttributes(getPlacement(table, useMasterPlacement), table.getAttributes());
    }

    private String getPlacement(Table table, boolean useOptionsPlacement) {
        if (!useOptionsPlacement) {
            TableAvailability availability = table.getAvailability();
            if (availability != null) {
                return availability.getPlacement();
            }
            // If the table isn't available locally then defer to it's placement from the table options.
            // If the user doesn't have permission the permission check will fail.  If he does the permission
            // check won't fail but another more informative exception will likely be thrown.
        }

        return table.getOptions().getPlacement();
    }
}
