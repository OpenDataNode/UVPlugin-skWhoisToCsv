package eu.comsode.unifiedviews.plugins.extractor.skwhoistocsv;

import eu.unifiedviews.dpu.config.DPUConfigException;
import eu.unifiedviews.helpers.dpu.vaadin.dialog.AbstractDialog;

public class SkWhoisToCsvVaadinDialog extends AbstractDialog<SkWhoisToCsvConfig_V1> {

    /**
     * 
     */
    private static final long serialVersionUID = -1539890500190667040L;

    public SkWhoisToCsvVaadinDialog() {
        super(SkWhoisToCsv.class);
    }

    @Override
    protected void buildDialogLayout() {
    }

    @Override
    protected void setConfiguration(SkWhoisToCsvConfig_V1 c) throws DPUConfigException {
    }

    @Override
    protected SkWhoisToCsvConfig_V1 getConfiguration() throws DPUConfigException {

        final SkWhoisToCsvConfig_V1 cnf = new SkWhoisToCsvConfig_V1();
        return cnf;
    }

}
