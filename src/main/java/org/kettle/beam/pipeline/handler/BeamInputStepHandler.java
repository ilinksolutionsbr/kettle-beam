package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamInputTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.steps.io.BeamInputMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamInputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  private String metaStoreJson;
  private boolean isGenericStep;
  private BeamGenericStepHandler genericStepHandler;

  public BeamInputStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, String metaStoreJson, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, true, false, metaStore, transMeta, stepPluginClasses, xpPluginClasses );
    this.metaStoreJson = metaStoreJson;
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta stepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input ) throws KettleException {

    // Input handling
    //
    BeamInputMeta beamInputMeta = (BeamInputMeta) stepMeta.getStepMetaInterface();
    FileDefinition inputFileDefinition = beamInputMeta.loadFileDefinition( metaStore );
    RowMetaInterface fileRowMeta = inputFileDefinition.getRowMeta();

    // Apply the PBegin to KettleRow transform:
    //
    if ( inputFileDefinition == null ) {
      throw new KettleException( "We couldn't find or load the Beam Input step file definition" );
    }
    String fileInputLocation = transMeta.environmentSubstitute( beamInputMeta.getInputLocation() );

    this.isGenericStep = inputFileDefinition.getSeparator() == null || inputFileDefinition.getSeparator().isEmpty();

    if(!this.isGenericStep) {
      BeamInputTransform beamInputTransform = new BeamInputTransform(
              stepMeta.getName(),
              stepMeta.getName(),
              fileInputLocation,
              transMeta.environmentSubstitute( Strings.isNullOrEmpty(inputFileDefinition.getSeparator()) ? "" : inputFileDefinition.getSeparator() ),
              JsonRowMeta.toJson( fileRowMeta ),
              stepPluginClasses,
              xpPluginClasses
      );
      PCollection<KettleRow> afterInput = pipeline.apply( beamInputTransform );
      stepCollectionMap.put( stepMeta.getName(), afterInput );
      log.logBasic( "Handled step (INPUT) : " + stepMeta.getName() );
    } else {
      this.getGenericStepHandler().handleStep(log, stepMeta, stepCollectionMap, pipeline, rowMeta, previousSteps, input);
    }
  }

  private BeamGenericStepHandler getGenericStepHandler(){
    if(this.genericStepHandler == null){
      this.genericStepHandler = new BeamGenericStepHandler( beamJobConfig, metaStore, metaStoreJson, transMeta, stepPluginClasses, xpPluginClasses );
    }
    return this.genericStepHandler;
  }

}
