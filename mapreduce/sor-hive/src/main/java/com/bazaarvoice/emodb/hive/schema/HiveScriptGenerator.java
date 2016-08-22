package com.bazaarvoice.emodb.hive.schema;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Common base class for Hive script generators.
 */
abstract public class HiveScriptGenerator {

    protected void doMain(String args[]) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser(getClass().getSimpleName());

        parser.addArgument("--out")
                .dest("out")
                .metavar("FILE")
                .nargs("?")
                .setDefault("stdout")
                .help("Writes the script to the output file; default is stdout");

        addArguments(parser);

        Namespace namespace = parser.parseArgsOrFail(args);
        String file = namespace.getString("out");

        try (PrintStream out = file.equals("stdout") ? System.out : new PrintStream(new FileOutputStream(file))) {
            generateScript(namespace, out);
        } catch (IOException e) {
            System.err.println("Script generation failed");
            e.printStackTrace(System.err);
        }
    }

    protected void addArguments(ArgumentParser parser) {
        // Default implementation does nothing
    }

    protected String escapeString(String str) {
        return str.replaceAll("'", "\\\\'");
    }

    /**
     * Defer creating the actual script to the subclass.
     */
    abstract protected void generateScript(Namespace namespace, PrintStream out);
}
