package org.kettle.beam.core.transform;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Duration;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.KettleToBQTableRowFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.Strings;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BeamBQOutputTransform extends PTransform<PCollection<KettleRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String stepname;
  private String projectId;
  private String datasetId;
  private String tableId;
  private String tempLocation;
  private String rowMetaJson;
  private boolean createIfNeeded;
  private boolean truncateTable;
  private boolean failIfNotEmpty;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamBQOutputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamOutputError" );

  public BeamBQOutputTransform() {
  }

  public BeamBQOutputTransform( String stepname, String projectId, String datasetId, String tableId, String tempLocation, boolean createIfNeeded, boolean truncateTable, boolean failIfNotEmpty, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    this.stepname = stepname;
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.tableId = tableId;
    this.tempLocation = tempLocation;
    this.createIfNeeded = createIfNeeded;
    this.truncateTable = truncateTable;
    this.failIfNotEmpty = failIfNotEmpty;
    this.rowMetaJson = rowMetaJson;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PDone expand( PCollection<KettleRow> input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamKettle.init( stepPluginClasses, xpPluginClasses );

      // Inflate the metadata on the node where this is running...
      //
      RowMetaInterface rowMeta = JsonRowMeta.fromJson( rowMetaJson );


      // Which table do we write to?
      //
      TableReference tableReference = new TableReference();
      if ( StringUtils.isNotEmpty( projectId ) ) {
        tableReference.setProjectId( projectId );
      }
      tableReference.setDatasetId( datasetId );
      tableReference.setTableId( tableId );

      TableSchema tableSchema = new TableSchema();
      List<TableFieldSchema> schemaFields = new ArrayList<>();
      for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
        TableFieldSchema schemaField = new TableFieldSchema();
        schemaField.setName( valueMeta.getName() );
        switch(valueMeta.getType()){
          case ValueMetaInterface.TYPE_STRING: schemaField.setType( "STRING" ); break;
          case ValueMetaInterface.TYPE_INTEGER: schemaField.setType( "INTEGER" ); break;
          case ValueMetaInterface.TYPE_DATE: schemaField.setType( "DATETIME" ); break;
          case ValueMetaInterface.TYPE_BOOLEAN: schemaField.setType( "BOOLEAN" ); break;
          case ValueMetaInterface.TYPE_NUMBER: schemaField.setType( "FLOAT" ); break;
          case ValueMetaInterface.TYPE_TIMESTAMP: schemaField.setType( "DATETIME" ); break;
          default:
            schemaField.setType( "STRING" ); break;
            //throw new RuntimeException( "Conversion from Kettle value "+valueMeta.toString()+" to BigQuery TableRow isn't supported yet" );
        }
        schemaFields.add(schemaField);
      }
      tableSchema.setFields( schemaFields );

      SerializableFunction<KettleRow, TableRow> formatFunction = new KettleToBQTableRowFn( stepname, rowMetaJson, stepPluginClasses, xpPluginClasses );

      BigQueryIO.Write.CreateDisposition createDisposition;
      if (createIfNeeded) {
        createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
      }  else {
        createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
      }

      BigQueryIO.Write.WriteDisposition writeDisposition;
      if (truncateTable) {
        writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
      } else {
        if (failIfNotEmpty) {
          writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_EMPTY;
        } else {
          writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
        }
      }

      BigQueryIO.Write<KettleRow> bigQueryWrite = BigQueryIO
        .<KettleRow>write()
        .to( tableReference )
        .withSchema( tableSchema )
        .withCreateDisposition( createDisposition )
        .withWriteDisposition( writeDisposition )
        .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
        .withFormatFunction( formatFunction );

      if(!Strings.isNullOrEmpty(this.tempLocation)){
        bigQueryWrite = bigQueryWrite.withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(this.tempLocation));
      }


      // TODO: pass the results along the way at some point
      //
      input.apply( stepname, bigQueryWrite );

      // End of the line
      //
      return PDone.in( input.getPipeline() );

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in Beam BigQuery output transform", e );
      throw new RuntimeException( "Error in Beam BigQuery output transform", e );
    }
  }
}
