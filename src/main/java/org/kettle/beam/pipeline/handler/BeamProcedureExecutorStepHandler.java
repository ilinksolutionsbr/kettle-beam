package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.steps.procedure.BeamProcedureExecutorMeta;
import org.kettle.beam.util.DatabaseUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeamProcedureExecutorStepHandler extends BeamBaseStepHandler implements BeamStepHandler, Serializable {

    private String metaStoreJson;
    private BeamGenericStepHandler genericStepHandler;

    public BeamProcedureExecutorStepHandler(BeamJobConfig beamJobConfig, IMetaStore metaStore, String metaStoreJson, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses) {
        super(beamJobConfig, true, false, metaStore, transMeta, stepPluginClasses, xpPluginClasses);
        this.metaStoreJson = metaStoreJson;
    }

    @Override
    public void handleStep(LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                           Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                           PCollection<KettleRow> input) throws KettleException {
        StepMeta previousStep;

        if (previousSteps == null || previousSteps.size() <= 1) {
            previousStep = previousSteps != null && previousSteps.size() == 1 ? previousSteps.get(0) : null;
        } else {
            throw new KettleException("Combining data from multiple steps is not supported yet!");
        }

        BeamProcedureExecutorMeta metadata = (BeamProcedureExecutorMeta) stepMeta.getStepMetaInterface();
        String rowMetaJson = rowMeta != null ? JsonRowMeta.toJson(rowMeta) : null;

        List<String> parameters = new ArrayList<>();
        String sql = DatabaseUtil.prepareSQL(metadata.getQuery(), parameters);

        Map<String, String> configuration = new HashMap<>();
        for(String parameter : parameters){
            configuration.put(parameter, this.transMeta.environmentSubstitute("${" + parameter + "}"));
        }

        this.getGenericStepHandler().handleStep(log, stepMeta, stepCollectionMap, pipeline, rowMeta, previousSteps, input);
    }

    private BeamGenericStepHandler getGenericStepHandler(){
        if(this.genericStepHandler == null){
            this.genericStepHandler = new BeamGenericStepHandler( beamJobConfig, metaStore, metaStoreJson, transMeta, stepPluginClasses, xpPluginClasses );
        }
        return this.genericStepHandler;
    }
}
