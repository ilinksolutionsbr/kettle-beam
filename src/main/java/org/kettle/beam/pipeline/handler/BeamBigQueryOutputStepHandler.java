package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamBQInputTransform;
import org.kettle.beam.core.transform.BeamBQOutputTransform;
import org.kettle.beam.core.transform.BeamOutputTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.bq.BeamBQOutputMeta;
import org.kettle.beam.steps.io.BeamOutputMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamBigQueryOutputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  private String metaStoreJson;
  private boolean isGenericStep;
  private BeamGenericStepHandler genericStepHandler;

  public BeamBigQueryOutputStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, String metaStoreJson, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, false, true, metaStore, transMeta, stepPluginClasses, xpPluginClasses );
    this.metaStoreJson = metaStoreJson;
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input  ) throws KettleException {

    BeamBQOutputMeta beamOutputMeta = (BeamBQOutputMeta) stepMeta.getStepMetaInterface();
    // Which step do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if (previousSteps.size() > 1) {
      throw new KettleException("Combining data from multiple steps is not supported yet!");
    }
    StepMeta previousStep = previousSteps.get(0);

    this.isGenericStep = !Strings.isNullOrEmpty(beamOutputMeta.getQuery());

    if (!this.isGenericStep) {
      BeamBQOutputTransform beamOutputTransform = new BeamBQOutputTransform(
              stepMeta.getName(),
              transMeta.environmentSubstitute(beamOutputMeta.getProjectId()),
              transMeta.environmentSubstitute(beamOutputMeta.getDatasetId()),
              transMeta.environmentSubstitute(beamOutputMeta.getTableId()),
              transMeta.environmentSubstitute(beamOutputMeta.getTempLocation()),
              beamOutputMeta.isCreatingIfNeeded(),
              beamOutputMeta.isTruncatingTable(),
              beamOutputMeta.isFailingIfNotEmpty(),
              JsonRowMeta.toJson(rowMeta),
              stepPluginClasses,
              xpPluginClasses
      );
      // No need to store this, it's PDone.
      //
      input.apply(beamOutputTransform);

    } else {
      this.getGenericStepHandler().handleStep(log, stepMeta, stepCollectionMap, pipeline, rowMeta, previousSteps, input);
    }

    log.logBasic("Handled step (BQ OUTPUT) : " + stepMeta.getName() + ", gets data from " + previousStep.getName());

  }

  private BeamGenericStepHandler getGenericStepHandler(){
    if(this.genericStepHandler == null){
      this.genericStepHandler = new BeamGenericStepHandler( beamJobConfig, metaStore, metaStoreJson, transMeta, stepPluginClasses, xpPluginClasses );
    }
    return this.genericStepHandler;
  }



}

