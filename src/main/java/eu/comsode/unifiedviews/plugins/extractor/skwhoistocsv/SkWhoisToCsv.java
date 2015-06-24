package eu.comsode.unifiedviews.plugins.extractor.skwhoistocsv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.files.FilesDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dataunit.files.FilesHelper;
import eu.unifiedviews.helpers.dpu.config.ConfigHistory;
import eu.unifiedviews.helpers.dpu.context.ContextUtils;
import eu.unifiedviews.helpers.dpu.exec.AbstractDpu;
import eu.unifiedviews.helpers.dpu.rdf.EntityBuilder;

@DPU.AsExtractor
public class SkWhoisToCsv extends AbstractDpu<SkWhoisToCsvConfig_V1> {
    public static Pattern PATTERN = Pattern.compile("(.*?)   *(.*)");

    private static final Logger LOG = LoggerFactory.getLogger(SkWhoisToCsv.class);

    private static final String BASE_URI = "http://localhost/";

    private static final int REPEAT_COUNT = 30;

    private static int WAIT_IN_MILIS = 100;

    private Map<String, Integer> keys = new HashMap<String, Integer>();

    @DataUnit.AsInput(name = "filesInput")
    public FilesDataUnit filesInput;

    @DataUnit.AsOutput(name = "rdfOutput")
    public WritableRDFDataUnit rdfOutput;

    public SkWhoisToCsv() {
        super(SkWhoisToCsvVaadinDialog.class, ConfigHistory.noHistory(SkWhoisToCsvConfig_V1.class));
    }

    @Override
    protected void innerExecute() throws DPUException {
        initializeKeysMap();
        try {
            Set<FilesDataUnit.Entry> inputFiles = FilesHelper.getFiles(filesInput);

            FilesDataUnit.Entry entry = inputFiles.iterator().next();
            File inputFile = new File(URI.create(entry.getFileURIString()));
            int index = 1;
            RepositoryConnection connection = null;
            try (BufferedReader inputReader = new BufferedReader(new FileReader(inputFile))) {
                org.openrdf.model.URI graph = rdfOutput.addNewDataGraph("skWhoisRdfData");
                connection = rdfOutput.getConnection();
                ValueFactory vf = ValueFactoryImpl.getInstance();
                String line = null;
                boolean firstLineInput = true;
                while ((line = inputReader.readLine()) != null) {
                    if (firstLineInput) {
                        firstLineInput = false;
                        continue;
                    }
                    String inputId = new String(line);
                    UUID uuid = UUID.randomUUID();
                    org.openrdf.model.URI uri = vf.createURI(BASE_URI + uuid.toString());
                    EntityBuilder eb = new EntityBuilder(uri, vf);
                    eb.property(RDF.TYPE, vf.createURI(BASE_URI + "Record"));
                    int failCount = 1;
                    while (true) {
                        try {
                            Thread.sleep(WAIT_IN_MILIS);
                        } catch (InterruptedException ex) {
                            LOG.error("Sleep for " + WAIT_IN_MILIS + " milliseconds interrupted.");
                            Thread.currentThread().interrupt();
                            throw ContextUtils.dpuExceptionCancelled(ctx);
                        }

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
                                if (StringUtils.contains(whoisLine, "Not found.")) {
                                    LOG.error("ID " + inputId + " was not found!");
                                    break;
                                }
                                Matcher m = PATTERN.matcher(whoisLine);
                                if (m.matches()) {
                                    String key = m.group(1).replaceAll(" ", "-");
                                    if (!keys.containsKey(key)) {
                                        LOG.error("Unknown key " + key + " with value " + m.group(2) + " found on record of " + line);
                                    } else {
                                        keys.put(key, keys.get(key) + 1);
                                    }
                                    eb.property(vf.createURI(BASE_URI + key), m.group(2));
                                } else {
                                    throw ContextUtils.dpuException(ctx, "FilesFilter.innerExecute.format", whoisLine);
                                }
                            }
                            if (whoisLines.size() != 0) {
                                LOG.info("Reading data for ID: " + inputId + " index: " + index);
                                WAIT_IN_MILIS = Math.max(WAIT_IN_MILIS / 4, 1);
                                connection.add(eb.asStatements(), graph);
                                break;
                            } else if (whoisLines.size() == 0 && failCount == REPEAT_COUNT) {
                                WAIT_IN_MILIS *= 2;
                                LOG.error("Error reading data for ID " + inputId + "!");
                                break;
                            } else {
                                WAIT_IN_MILIS *= 2;
                                LOG.warn("Error reading data for ID " + inputId + ". Trying to wait for " + WAIT_IN_MILIS + " milliseconds.");
                                failCount++;
                            }
                        }
                    }
                    index++;
                }
            } catch (IOException ex) {
                ContextUtils.dpuException(ctx, ex, "FilesFilter.innerExecute.exception");
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (RepositoryException ex) {
                        LOG.warn("Error closing connection.", ex);
                    }
                }
            }
            for (Map.Entry<String, Integer> e : keys.entrySet()) {
                LOG.info("Parameter " + e.getKey() + " was filled " + e.getValue() + " times.");
            }
        } catch (DataUnitException | RepositoryException ex) {
            ContextUtils.dpuException(ctx, ex, "FilesFilter.innerExecute.exception");
        }
    }

    private void initializeKeysMap() {
        keys.put("Contact-name", 0);
        keys.put("Organization", 0);
        keys.put("Legal-form", 0);
        keys.put("Organization-ID", 0);
        keys.put("Work-telephone", 0);
        keys.put("Fax-telephone", 0);
        keys.put("Work-address", 0);
        keys.put("Email-address", 0);
        keys.put("Handle", 0);
        keys.put("User-delegation", 0);
        keys.put("Last-record-update", 0);
    }
}
