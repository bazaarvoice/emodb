package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultDataCopyDAO implements DataCopyDAO {

    private final DataCopyWriterDAO _dataCopyWriterDAO;
    private final DataCopyReaderDAO _dataCopyReaderDAO;

    @Inject
    public DefaultDataCopyDAO(DataCopyWriterDAO dataCopyWriterDAO, DataCopyReaderDAO dataCopyReaderDAO) {
        _dataCopyWriterDAO = checkNotNull(dataCopyWriterDAO);
        _dataCopyReaderDAO = checkNotNull(dataCopyReaderDAO);
    }

    @Override
    public void copy(AstyanaxStorage source, AstyanaxStorage dest, Runnable progress) {
        checkNotNull(source, "source");
        checkNotNull(dest, "dest");

        _dataCopyWriterDAO.copyDeltasToDestination(_dataCopyReaderDAO.getDeltasForStorage(source), dest, progress);
        _dataCopyWriterDAO.copyHistoriesToDestination(_dataCopyReaderDAO.getHistoriesForStorage(source), dest, progress);
    }
}
