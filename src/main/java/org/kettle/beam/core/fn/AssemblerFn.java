package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AssemblerFn extends DoFn<KV<KettleRow, KV<KettleRow, KettleRow>>, KettleRow> {

  private String outputRowMetaJson;
  private String leftKRowMetaJson;
  private String leftVRowMetaJson;
  private String rightVRowMetaJson;

  private String leftRowMetaJson;
  private String rightRowMetaJson;

  private String counterName;
  private List<String> stepPluginClasses;
  private List<String>xpPluginClasses;

  private static final Logger LOG = LoggerFactory.getLogger( AssemblerFn.class );

  private transient RowMetaInterface outputRowMeta;
  private transient RowMetaInterface leftKRowMeta;
  private transient RowMetaInterface leftVRowMeta;
  private transient RowMetaInterface rightVRowMeta;

  private transient RowMetaInterface leftRowMeta;
  private transient RowMetaInterface rightRowMeta;

  private transient Counter initCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  public AssemblerFn() {
  }

  public AssemblerFn( String outputRowMetaJson, String leftKRowMetaJson, String leftVRowMetaJson, String rightVRowMetaJson,
                      String leftRowMetaJson, String rightRowMetaJson,
                      String counterName, List<String> stepPluginClasses, List<String>xpPluginClasses) {
    this.outputRowMetaJson = outputRowMetaJson;
    this.leftKRowMetaJson = leftKRowMetaJson;
    this.leftVRowMetaJson = leftVRowMetaJson;
    this.rightVRowMetaJson = rightVRowMetaJson;

    this.leftRowMetaJson = leftRowMetaJson;
    this.rightRowMetaJson = rightRowMetaJson;

    this.counterName = counterName;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Setup
  public void setUp() {
    try {
      writtenCounter = Metrics.counter( "written", counterName );
      errorCounter = Metrics.counter( "error", counterName );

      // Initialize Kettle Beam
      //
      BeamKettle.init( stepPluginClasses, xpPluginClasses );
      outputRowMeta = JsonRowMeta.fromJson( outputRowMetaJson );
      leftKRowMeta = JsonRowMeta.fromJson( leftKRowMetaJson );
      leftVRowMeta = JsonRowMeta.fromJson( leftVRowMetaJson );
      rightVRowMeta = JsonRowMeta.fromJson( rightVRowMetaJson );

      leftRowMeta = JsonRowMeta.fromJson( leftRowMetaJson );
      rightRowMeta = JsonRowMeta.fromJson( rightRowMetaJson );

      Metrics.counter( "init", counterName ).inc();
    } catch(Exception e) {
      errorCounter.inc();
      LOG.error( "Error initializing assembling rows", e);
      throw new RuntimeException( "Error initializing assembling output KV<row, KV<row, row>>", e );
    }
  }

  //TODO: novo
  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      KV<KettleRow, KV<KettleRow, KettleRow>> element = processContext.element();
      KV<KettleRow, KettleRow> value = element.getValue();

      KettleRow key = element.getKey();
      KettleRow leftValue = value.getKey();
      KettleRow rightValue = value.getValue();

      Object[] outputRow = RowDataUtil.allocateRowData( leftRowMeta.size() +  rightRowMeta.size());
      int rowIndex = 0;
      int keyIndex;
      int valueIndex = 0;
      Object rowValue;
      boolean isKey;

      //Inclui os dados da esquerda
      if(!leftValue.allNull()) {
        for (ValueMetaInterface valueMeta : leftRowMeta.getValueMetaList()) {
          keyIndex = 0;
          rowValue = null;
          isKey = false;

          for (ValueMetaInterface keyValueMeta : leftKRowMeta.getValueMetaList()) {
            if (valueMeta.getName().equalsIgnoreCase(keyValueMeta.getName())) {
              rowValue = key.getRow()[keyIndex];
              isKey = true;
              break;
            }
            keyIndex++;
          }

          if (!isKey) {
            rowValue = leftValue.getRow()[valueIndex++];
          }

          outputRow[rowIndex++] = rowValue;
        }
      }else{
        rowIndex += leftRowMeta.getValueMetaList().size();
      }

      //Inclui os dados da direita
      if(!rightValue.allNull()) {
        valueIndex = 0;
        for (ValueMetaInterface valueMeta : rightRowMeta.getValueMetaList()) {
          keyIndex = 0;
          rowValue = null;
          isKey = false;

          for (ValueMetaInterface keyValueMeta : leftKRowMeta.getValueMetaList()) {
            if (valueMeta.getName().equalsIgnoreCase(keyValueMeta.getName())) {
              rowValue = key.getRow()[keyIndex];
              isKey = true;
              break;
            }
            keyIndex++;
          }

          if (!isKey) {
            rowValue = rightValue.getRow()[valueIndex++];
          }

          outputRow[rowIndex++] = rowValue;
        }
      }else{
        rowIndex += rightRowMeta.getValueMetaList().size();
      }

      //Processa os dados
      processContext.output( new KettleRow( outputRow ) );
      writtenCounter.inc();

    } catch(Exception e) {
      errorCounter.inc();
      LOG.error( "Error assembling rows", e);
      throw new RuntimeException( "Error assembling output KV<row, KV<row, row>>", e );
    }
  }

/*
  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      KV<KettleRow, KV<KettleRow, KettleRow>> element = processContext.element();
      KV<KettleRow, KettleRow> value = element.getValue();

      KettleRow key = element.getKey();
      KettleRow leftValue = value.getKey();
      KettleRow rightValue = value.getValue();

      Object[] outputRow = RowDataUtil.allocateRowData( outputRowMeta.size() );
      int index = 0;

      // Kettle style, first the left values
      //
      if (leftValue.allNull()) {
        index+=leftVRowMeta.size();
      } else {
        for ( int i = 0; i < leftVRowMeta.size(); i++ ) {
          outputRow[ index++ ] = leftValue.getRow()[ i ];
        }
      }

      // Now the left key
      //
      if (leftValue.allNull()) {
        index+=leftKRowMeta.size();
      } else {
        for ( int i = 0; i < leftKRowMeta.size(); i++ ) {
          outputRow[ index++ ] = key.getRow()[ i ];
        }
      }

      // Then the right key
      //
      if (rightValue.allNull()) {
        // No right key given if the value is null
        //
        index+=leftKRowMeta.size();
      } else {
        for ( int i = 0; i < leftKRowMeta.size(); i++ ) {
          outputRow[ index++ ] = key.getRow()[ i ];
        }
      }

      // Finally the right values
      //
      if (rightValue.allNull()) {
        index+=rightVRowMeta.size();
      } else {
        for ( int i = 0; i < rightVRowMeta.size(); i++ ) {
          outputRow[ index++ ] = rightValue.getRow()[ i ];
        }
      }

      // System.out.println("Assembled row : "+outputRowMeta.getString(outputRow));

      processContext.output( new KettleRow( outputRow ) );
      writtenCounter.inc();

    } catch(Exception e) {
      errorCounter.inc();
      LOG.error( "Error assembling rows", e);
      throw new RuntimeException( "Error assembling output KV<row, KV<row, row>>", e );
    }
  }
*/

}

