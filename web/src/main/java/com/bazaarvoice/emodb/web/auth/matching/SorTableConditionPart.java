package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.condition.Condition;

import javax.annotation.Nullable;

/**
 * {@link TableConditionPart} implementation for evaluating system of record tables.
 */
public class SorTableConditionPart extends TableConditionPart {

    private final DataStore _dataStore;

    public SorTableConditionPart(Condition condition, DataStore dataStore) {
        super(condition);
        _dataStore = dataStore;
    }

    @Nullable
    @Override
    protected PlacementAndAttributes getPlacementAndAttributesForTable(String tableName, boolean useMasterPlacement) {
        // In some Emo configurations there may legitimately not be a data store.  If this is the case
        // don't raise an exception, just deny permission.
        if (_dataStore == null) {
            return null;
        }

        Table table = _dataStore.getTableMetadata(tableName);
        return new PlacementAndAttributes(getPlacement(table, useMasterPlacement), table.getTemplate());
    }

    private String getPlacement(Table table, boolean useMasterPlacement) {
        if (!useMasterPlacement) {
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
