package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;

import java.util.Iterator;

interface AstyanaxKeyScanner {
    Iterator<String> scanKeys(AstyanaxStorage storage, final ReadConsistency consistency);
}
