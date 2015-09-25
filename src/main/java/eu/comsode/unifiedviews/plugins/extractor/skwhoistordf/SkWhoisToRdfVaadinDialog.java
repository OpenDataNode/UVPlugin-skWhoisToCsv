package eu.comsode.unifiedviews.plugins.extractor.skwhoistordf;

import eu.unifiedviews.dpu.config.DPUConfigException;
import eu.unifiedviews.helpers.dpu.vaadin.dialog.AbstractDialog;

public class SkWhoisToRdfVaadinDialog extends AbstractDialog<SkWhoisToRdfConfig_V1> {

    /**
     * 
     */
    private static final long serialVersionUID = -1539890500190667040L;

    public SkWhoisToRdfVaadinDialog() {
        super(SkWhoisToRdf.class);
    }

    @Override
    protected void buildDialogLayout() {
    }

    @Override
    protected void setConfiguration(SkWhoisToRdfConfig_V1 c) throws DPUConfigException {
    }

    @Override
    protected SkWhoisToRdfConfig_V1 getConfiguration() throws DPUConfigException {

        final SkWhoisToRdfConfig_V1 cnf = new SkWhoisToRdfConfig_V1();
        return cnf;
    }

}
