package com.bazaarvoice.emodb.sor.admin;

import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.PrintWriter;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Converts EmoDB coordinates "table/key" to a Cassandra row key and vice versa.
 * <p>
 * Example usage:
 * <pre>
 * $ curl -s -XPOST http://localhost:8081/tasks/sor-row-key?coord=review:testcustomer/demo1
 * review:testcustomer/demo1: 564c0c4f54555e41e664656d6f31
 *
 * $ curl -s -XPOST http://localhost:8081/tasks/sor-row-key?rowkey=564c0c4f54555e41e664656d6f31
 * 564c0c4f54555e41e664656d6f31: review:testcustomer/demo1
 * </pre>
 */
public class RowKeyTask extends Task {
    private final TableDAO _tableDao;

    @Inject
    public RowKeyTask(TaskRegistry taskRegistry, TableDAO tableDao) {
        super("sor-row-key");
        _tableDao = requireNonNull(tableDao, "tableDao");
        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter out) {
        for (String coordString : parameters.get("coord")) {
            try {
                Coordinate coord = Coordinate.parse(coordString);
                Table table = _tableDao.get(coord.getTable());
                for (AstyanaxStorage storage : ((AstyanaxTable) table).getWriteStorage()) {
                    ByteBuffer rowKey = storage.getRowKey(coord.getId());
                    out.printf("%s: %s%n", coord, ByteBufferUtil.bytesToHex(rowKey));
                }
            } catch (Exception e) {
                out.println(e); // Likely an invalid table or coordinate
            }
        }

        for (String rowkeyString : parameters.get("rowkey")) {
            String coord;
            try {
                ByteBuffer rowkey = ByteBufferUtil.hexToBytes(rowkeyString);
                long tableUuid = AstyanaxStorage.getTableUuid(rowkey);
                Table table = _tableDao.getByUuid(tableUuid);
                coord = table.getName() + "/" + AstyanaxStorage.getContentKey(rowkey);
            } catch (NumberFormatException e) {
                coord = "invalid hex";
            } catch (UnknownTableException | DroppedTableException e) {
                coord = "unknown table UUID";
            } catch (Exception e) {
                out.println(e);
                coord = "error";
            }
            out.printf("%s: %s%n", rowkeyString, coord);
        }
    }
}
