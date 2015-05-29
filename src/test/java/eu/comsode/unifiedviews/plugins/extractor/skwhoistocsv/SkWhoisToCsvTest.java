package eu.comsode.unifiedviews.plugins.extractor.skwhoistocsv;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;

import cz.cuni.mff.xrg.odcs.dpu.test.TestEnvironment;
import eu.unifiedviews.dataunit.files.WritableFilesDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.helpers.dataunit.rdf.RDFHelper;
import eu.unifiedviews.helpers.dpu.test.config.ConfigurationBuilder;

public class SkWhoisToCsvTest {

    @Test
    public void execute() throws Exception {
        // Prepare config.
        SkWhoisToCsvConfig_V1 config = new SkWhoisToCsvConfig_V1();

        SkWhoisToCsv dpu = new SkWhoisToCsv();
        // Prepare DPU.
        dpu.configure((new ConfigurationBuilder()).setDpuConfiguration(config).toString());

        // Prepare test environment.
        TestEnvironment environment = new TestEnvironment();

        // Prepare data unit.
        WritableRDFDataUnit rdfOutput = environment.createRdfOutput("rdfOutput", false);
        WritableFilesDataUnit filesInput = environment.createFilesInput("filesInput");
        InputStream inputFileIS = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("codes.txt");
        InputStream outputFileIS = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("codes_output.txt");

        String s1 = IOUtils.toString(outputFileIS);
        s1 = s1.substring(s1.indexOf('\n') + 1);
        s1 = s1.substring(s1.indexOf('\n') + 1);

        File tempFile = File.createTempFile("____", "fdsa");
        FileUtils.copyInputStreamToFile(inputFileIS, tempFile);
        try {
            filesInput.addExistingFile("codes.txt", URI.create(tempFile.toURI().toASCIIString()).toString());

            // Run.
            environment.run(dpu);

            RepositoryConnection con = rdfOutput.getConnection();
            StringWriter sw = new StringWriter();
            con.export(Rio.createWriter(RDFFormat.TURTLE, sw), RDFHelper.getGraphsURIArray(rdfOutput));
            String s2 = sw.toString();
            s2 = s2.substring(s2.indexOf('\n') + 1);
            s2 = s2.substring(s2.indexOf('\n') + 1);
            Assert.assertEquals(s1.trim(), s2.trim());
        } finally {
            // Release resources.
            environment.release();
        }
    }

    @Test
    public void execute2() throws Exception {
        // Prepare config.
        SkWhoisToCsvConfig_V1 config = new SkWhoisToCsvConfig_V1();

        SkWhoisToCsv dpu = new SkWhoisToCsv();
        // Prepare DPU.
        dpu.configure((new ConfigurationBuilder()).setDpuConfiguration(config).toString());

        // Prepare test environment.
        TestEnvironment environment = new TestEnvironment();

        // Prepare data unit.
        WritableRDFDataUnit rdfOutput = environment.createRdfOutput("rdfOutput", false);
        WritableFilesDataUnit filesInput = environment.createFilesInput("filesInput");
        InputStream inputFileIS = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("domeny_100.txt");
        InputStream outputFileIS = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("codes_output.txt");

        File tempFile = File.createTempFile("____", "fdsa");
        FileUtils.copyInputStreamToFile(inputFileIS, tempFile);
        try {
            filesInput.addExistingFile("domeny.txt", URI.create(tempFile.toURI().toASCIIString()).toString());

            // Run.
            environment.run(dpu);

            RepositoryConnection con = rdfOutput.getConnection();
            con.export(Rio.createWriter(RDFFormat.TURTLE, System.out), RDFHelper.getGraphsURIArray(rdfOutput));
        } finally {
            // Release resources.
            environment.release();
        }
    }
}
