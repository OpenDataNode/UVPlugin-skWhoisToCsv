package eu.comsode.unifiedviews.plugins.extractor.skwhoistocsv;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import cz.cuni.mff.xrg.odcs.dpu.test.TestEnvironment;
import eu.comsode.unifiedviews.plugins.extractor.skwhoistocsv.SkWhoisToCsv;
import eu.comsode.unifiedviews.plugins.extractor.skwhoistocsv.SkWhoisToCsvConfig_V1;
import eu.unifiedviews.dataunit.files.FilesDataUnit;
import eu.unifiedviews.dataunit.files.WritableFilesDataUnit;
import eu.unifiedviews.helpers.dataunit.files.FilesHelper;
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
        WritableFilesDataUnit filesOutput = environment.createFilesOutput("filesOutput");
        WritableFilesDataUnit filesInput = environment.createFilesInput("filesInput");
        InputStream inputFileIS = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("codes.txt");
        InputStream outputFileIS = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("codes_output.txt");
        byte[] outputFileArray = IOUtils.toByteArray(outputFileIS);
        
        File tempFile = File.createTempFile("____", "fdsa");
        FileUtils.copyInputStreamToFile(inputFileIS, tempFile);
        try {
            filesInput.addExistingFile("codes.txt", URI.create(tempFile.toURI().toASCIIString()).toString());

            // Run.
            environment.run(dpu);

            // Get file iterator.
            Set<FilesDataUnit.Entry> outputEntries = FilesHelper.getFiles(filesOutput);

            // Iterate over files.
            for (FilesDataUnit.Entry entry : outputEntries) {
                byte[] outputContent = FileUtils.readFileToByteArray(new File(new URI(entry.getFileURIString())));
                Assert.assertArrayEquals(outputFileArray, outputContent);
            }
        } finally {
            // Release resources.
            environment.release();
        }
    }
}
