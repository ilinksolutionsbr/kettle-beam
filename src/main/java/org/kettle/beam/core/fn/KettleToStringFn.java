package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KettleToStringFn extends DoFn<KettleRow, String> {

  private FileDefinition fileDefinition;
  private RowMetaInterface rowMeta;

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger( KettleToStringFn.class );
    private final Counter numParseErrors = Metrics.counter( "main", "ParseErrors" );

    public KettleToStringFn( FileDefinition fileDefinition, RowMetaInterface rowMeta ) {
      this.fileDefinition = fileDefinition;
      this.rowMeta = rowMeta;
    }

    @ProcessElement
    public void processElement( ProcessContext processContext ) {
      KettleRow inputRow = processContext.element();

      try {

        // Just a quick and dirty output for now...
        // TODO: refine with mulitple output formats, Avro, Parquet, ...
        //
        StringBuffer line = new StringBuffer();

        for (int i=0;i<rowMeta.size();i++) {

          if (i>0) {
            line.append( fileDefinition.getSeparator() );
          }

          String valueString = rowMeta.getString( inputRow.getRow(), i );
          boolean enclose = false;

          if ( StringUtils.isNotEmpty(fileDefinition.getEnclosure())) {
            enclose = valueString.contains( fileDefinition.getEnclosure() );
          }
          if (enclose) {
            line.append( fileDefinition.getEnclosure() );
          }
          line.append( valueString );
          if (enclose) {
            line.append( fileDefinition.getEnclosure() );
          }
        }

        // Pass the row to the process context
        //
        processContext.output( line.toString() );

      } catch ( Exception e ) {
        numParseErrors.inc();
        LOG.info( "Parse error on " + processContext.element() + ", " + e.getMessage() );
      }
    }

  /**
   * Gets fileDefinition
   *
   * @return value of fileDefinition
   */
  public FileDefinition getFileDefinition() {
    return fileDefinition;
  }

  /**
   * @param fileDefinition The fileDefinition to set
   */
  public void setFileDefinition( FileDefinition fileDefinition ) {
    this.fileDefinition = fileDefinition;
  }

  /**
   * Gets rowMeta
   *
   * @return value of rowMeta
   */
  public RowMetaInterface getRowMeta() {
    return rowMeta;
  }

  /**
   * @param rowMeta The rowMeta to set
   */
  public void setRowMeta( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }
}