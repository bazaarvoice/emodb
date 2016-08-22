package com.bazaarvoice.emodb.hive.schema;

import com.bazaarvoice.emodb.hive.udf.EmoFunctions;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.PrintStream;
import java.util.Map;

import static java.lang.String.format;

/**
 * Creates an initial schema in Hive.
 */
public class CreateFunctions extends HiveScriptGenerator {

    private final static String CREATE_FUNCTION_COMMAND =
            "CREATE FUNCTION %s AS '%s';";

    public static void main(String args[]) {
        CreateFunctions instance = new CreateFunctions();
        instance.doMain(args);
    }

    @Override
    protected void generateScript(Namespace namespace, PrintStream out) {
        for (Map.Entry<String, Class<? extends UDF>> entry : EmoFunctions.ALL_FUNCTIONS.entrySet()) {
            String fcnName = entry.getKey();
            Class<? extends UDF> fcnClass = entry.getValue();

            out.println(format(CREATE_FUNCTION_COMMAND, fcnName, fcnClass.getName()));
            out.println();
        }
    }
}
