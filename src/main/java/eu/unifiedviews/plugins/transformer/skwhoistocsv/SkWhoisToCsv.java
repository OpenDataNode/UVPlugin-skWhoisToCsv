package eu.unifiedviews.plugins.transformer.skwhoistocsv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.files.FilesDataUnit;
import eu.unifiedviews.dataunit.files.WritableFilesDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dataunit.copy.CopyHelpers;
import eu.unifiedviews.helpers.dataunit.files.FilesHelper;
import eu.unifiedviews.helpers.dataunit.resource.Resource;
import eu.unifiedviews.helpers.dataunit.resource.ResourceHelpers;
import eu.unifiedviews.helpers.dpu.config.ConfigHistory;
import eu.unifiedviews.helpers.dpu.context.ContextUtils;
import eu.unifiedviews.helpers.dpu.exec.AbstractDpu;

@DPU.AsTransformer
public class SkWhoisToCsv extends AbstractDpu<SkWhoisToCsvConfig_V1> {
    public static Pattern PATTERN = Pattern.compile("(.*)   *(.*)");
    private static final Logger LOG = LoggerFactory.getLogger(SkWhoisToCsv.class);

    @DataUnit.AsInput(name = "filesInput")
    public FilesDataUnit filesInput;

    @DataUnit.AsOutput(name = "filesOutput")
    public WritableFilesDataUnit filesOutput;

    public SkWhoisToCsv() {
        super(SkWhoisToCsvVaadinDialog.class, ConfigHistory.noHistory(SkWhoisToCsvConfig_V1.class));
    }

    @Override
    protected void innerExecute() throws DPUException {
        try {
            Set<FilesDataUnit.Entry> inputFiles = FilesHelper.getFiles(filesInput);

            FilesDataUnit.Entry entry = inputFiles.iterator().next();
            File inputFile = new File(URI.create(entry.getFileURIString()));
            File outputFile = File.createTempFile("____", FilenameUtils.getExtension(inputFile.getAbsolutePath()), new File(URI.create(filesOutput.getBaseFileURIString())));
            int index = 1;
            try (BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));
                    PrintWriter outputWriter = new PrintWriter(new FileWriter(outputFile))) {
                String line = null;
                while ((line = inputReader.readLine()) != null) {
                    List<String> outputLine = new ArrayList<>();
                    try (Socket whoisClientSocket = new Socket()) {
                        line = line + "\r\n";
                        byte[] buffer = line.getBytes("US-ASCII");
                        whoisClientSocket.connect(new InetSocketAddress("whois.sk-nic.sk", 43));
                        whoisClientSocket.getOutputStream().write(buffer);
                        whoisClientSocket.getOutputStream().flush();
                        List<String> whoisLines = IOUtils.readLines(whoisClientSocket.getInputStream(), "US-ASCII");
                        for (String whoisLine : whoisLines) {
                            if (StringUtils.isBlank(whoisLine) || whoisLine.startsWith("%")) {
                                continue;
                            }
                            Matcher m = PATTERN.matcher(whoisLine);
                            if (m.matches()) {
                                outputLine.add(m.group(2));
                            } else {
                                throw ContextUtils.dpuException(ctx, "FilesFilter.innerExecute.format", whoisLine);
                            }
                        }
                    }
                    outputWriter.println(StringUtils.join(outputLine, ";"));
                    LOG.info("Done file {}", index);
                    index++;
                }
            } catch (IOException ex) {
                ContextUtils.dpuException(ctx, ex, "FilesFilter.innerExecute.exception");
            }
            CopyHelpers.copyMetadata(entry.getSymbolicName(), filesInput, filesOutput);
            Resource resource = ResourceHelpers.getResource(filesOutput, entry.getSymbolicName());
            resource.setLast_modified(new Date());
            resource.setMimetype("text/csv");
            resource.setSize(outputFile.length());
            ResourceHelpers.setResource(filesOutput, entry.getSymbolicName(), resource);
            filesOutput.updateExistingFileURI(entry.getSymbolicName(), outputFile.toURI().toASCIIString());
        } catch (DataUnitException | IOException ex) {
            ContextUtils.dpuException(ctx, ex, "FilesFilter.innerExecute.exception");
        }
    }
}
