package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class DefaultDataCopyDAO implements DataCopyDAO {

    private final DataCopyWriterDAO _dataCopyWriterDAO;
    private final DataCopyReaderDAO _dataCopyReaderDAO;

    @Inject
    public DefaultDataCopyDAO(DataCopyWriterDAO dataCopyWriterDAO, DataCopyReaderDAO dataCopyReaderDAO) {
        _dataCopyWriterDAO = requireNonNull(dataCopyWriterDAO);
        _dataCopyReaderDAO = requireNonNull(dataCopyReaderDAO);
    }

    @Override
    public void copy(AstyanaxStorage source, AstyanaxStorage dest, Runnable progress) {
        requireNonNull(source, "source");
        requireNonNull(dest, "dest");

        _dataCopyWriterDAO.copyDeltasToDestination(_dataCopyReaderDAO.getDeltasForStorage(source), dest, progress);
        _dataCopyWriterDAO.copyHistoriesToDestination(_dataCopyReaderDAO.getHistoriesForStorage(source), dest, progress);
    }
}
