package org.kettle.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamOutputTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
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

public class BeamOutputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamOutputStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, false, true, metaStore, transMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( LogChannelInterface log, StepMeta beamOutputStepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                                    Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                                    PCollection<KettleRow> input ) throws KettleException {

    BeamOutputMeta beamOutputMeta = (BeamOutputMeta) beamOutputStepMeta.getStepMetaInterface();
    FileDefinition outputFileDefinition = beamOutputMeta.parseFileDefinition();

    // Empty file definition? Add all fields in the output
    //
    addAllFieldsToEmptyFileDefinition( rowMeta, outputFileDefinition );

    // Apply the output transform from KettleRow to PDone
    //
    if ( outputFileDefinition == null ) {
      throw new KettleException( "We couldn't find or load the Beam Output step file definition" );
    }
    if ( rowMeta == null || rowMeta.isEmpty() ) {
      throw new KettleException( "No output fields found in the file definition or from previous steps" );
    }

    BeamOutputTransform beamOutputTransform = new BeamOutputTransform(
      beamOutputStepMeta.getName(),
      transMeta.environmentSubstitute( beamOutputMeta.getOutputLocation() ),
      transMeta.environmentSubstitute( beamOutputMeta.getFilePrefix() ),
      transMeta.environmentSubstitute( beamOutputMeta.getFileSuffix() ),
      beamOutputMeta.isWindowed(),
      JsonRowMeta.toJson( rowMeta ),
      stepPluginClasses,
      xpPluginClasses,
      outputFileDefinition
    );

    // Which step do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if ( previousSteps.size() > 1 ) {
      throw new KettleException( "Combining data from multiple steps is not supported yet!" );
    }
    StepMeta previousStep = previousSteps.get( 0 );

    // No need to store this, it's PDone.
    //
    input.apply( beamOutputTransform );
    log.logBasic( "Handled step (OUTPUT) : " + beamOutputStepMeta.getName() + ", gets data from " + previousStep.getName() );
  }

  private FileDefinition getDefaultFileDefition( StepMeta beamOutputStepMeta ) throws KettleStepException {
    FileDefinition fileDefinition = new FileDefinition();

    fileDefinition.setName( "Default" );
    fileDefinition.setEnclosure( "\"" );
    fileDefinition.setSeparator( "," );

    return fileDefinition;
  }

  private void addAllFieldsToEmptyFileDefinition( RowMetaInterface rowMeta, FileDefinition fileDefinition ) throws KettleStepException {
    if ( fileDefinition.getFieldDefinitions().isEmpty() ) {
      for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
        fileDefinition.getFieldDefinitions().add( new FieldDefinition(
            valueMeta.getName(),
            valueMeta.getTypeDesc(),
            valueMeta.getLength(),
            valueMeta.getPrecision(),
            valueMeta.getConversionMask()
          )
        );
      }
    }
  }

}
