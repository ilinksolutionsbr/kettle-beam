package org.kettle.beam.pipeline.handler;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamDatabaseConnectorQueryTransform;
import org.kettle.beam.core.transform.BeamDatabaseConnectorUpdateTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.steps.database.BeamDatabaseConnectorHelper;
import org.kettle.beam.steps.database.BeamDatabaseConnectorMeta;
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

public class BeamDatabaseConnectorHandler extends BeamBaseStepHandler implements BeamStepHandler, Serializable {

    //region Constructors

    public BeamDatabaseConnectorHandler(BeamJobConfig beamJobConfig, IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses) {
        super(beamJobConfig, true, true, metaStore, transMeta, stepPluginClasses, xpPluginClasses);
    }

    //endregion


    //region Methods

    @Override
    public void handleStep(LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                           Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                           PCollection<KettleRow> input) throws KettleException {
        StepMeta previousStep;

        if (previousSteps == null || previousSteps.size() <= 1) {
            previousStep = previousSteps != null && previousSteps.size() == 1 ? previousSteps.get(0) : null;
        }else {
            throw new KettleException("Combining data from multiple steps is not supported yet!");
        }

        BeamDatabaseConnectorMeta metadata = (BeamDatabaseConnectorMeta) stepMeta.getStepMetaInterface();
        String rowMetaJson = rowMeta != null ? JsonRowMeta.toJson(rowMeta) : null;

        List<String> parameters = new ArrayList<>();
        String sql = BeamDatabaseConnectorHelper.getInstance().prepareSQL(metadata.getQuery(), parameters);

        Map<String, String> configuration = new HashMap<>();
        for(String parameter : parameters){
            configuration.put(parameter, this.transMeta.environmentSubstitute("${" + parameter + "}"));
        }

        String queryType = this.transMeta.environmentSubstitute(metadata.getQueryType());

        if(BeamDatabaseConnectorHelper.QUERY_TYPE_SELECT.equalsIgnoreCase(queryType)){
            BeamDatabaseConnectorQueryTransform transform = new BeamDatabaseConnectorQueryTransform(
                    stepMeta.getName()
                    , metadata.getDatabase()
                    , BeamDatabaseConnectorHelper.getInstance().getDriver(metadata.getDatabase())
                    , this.transMeta.environmentSubstitute(metadata.getConnectionString())
                    , this.transMeta.environmentSubstitute(metadata.getUsername())
                    , this.transMeta.environmentSubstitute(metadata.getPassword())
                    , this.transMeta.environmentSubstitute(metadata.getQueryType())
                    , sql
                    , metadata.getFields()
                    , parameters
                    , configuration
                    , rowMetaJson
                    , this.stepPluginClasses
                    , this.xpPluginClasses);
            PCollection<KettleRow> output = pipeline.apply(transform);
            stepCollectionMap.put(stepMeta.getName(), output);

        }else{

            BeamDatabaseConnectorUpdateTransform transform = new BeamDatabaseConnectorUpdateTransform(
                    stepMeta.getName()
                    , metadata.getDatabase()
                    , BeamDatabaseConnectorHelper.getInstance().getDriver(metadata.getDatabase())
                    , this.transMeta.environmentSubstitute(metadata.getConnectionString())
                    , this.transMeta.environmentSubstitute(metadata.getUsername())
                    , this.transMeta.environmentSubstitute(metadata.getPassword())
                    , this.transMeta.environmentSubstitute(metadata.getQueryType())
                    , sql
                    , metadata.getFields()
                    , parameters
                    , configuration
                    , rowMetaJson
                    , this.stepPluginClasses
                    , this.xpPluginClasses);
            input.apply(transform);

        }

        log.logBasic("Handled step (DATABASE CONNECTOR) : " + stepMeta.getName() + (previousStep != null ? ", gets data from " + previousStep.getName() : ""));

    }

    //endregion

}
