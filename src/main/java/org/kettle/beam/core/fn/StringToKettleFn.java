package org.kettle.beam.core.fn;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StringToKettleFn extends DoFn<String, KettleRow> {

  private String stepname;
  private String rowMetaJson;
  private String separator;
  private List<String> stepPluginClasses;
  private List<String> xpPluginClasses;
  private FileDefinition fileDefinition;

  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger( StringToKettleFn.class );

  private transient RowMetaInterface rowMeta;

  public StringToKettleFn( String stepname, String rowMetaJson, String separator, List<String> stepPluginClasses, List<String> xpPluginClasses, FileDefinition fileDefinition ) {
    this.stepname = stepname;
    this.rowMetaJson = rowMetaJson;
    this.separator = separator;
    this.stepPluginClasses = stepPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.fileDefinition = fileDefinition;
  }

  @Setup
  public void setUp() {
    try {
      inputCounter = Metrics.counter( "input", stepname );
      writtenCounter = Metrics.counter( "written", stepname );

      // Initialize Kettle Beam
      //
      BeamKettle.init( stepPluginClasses, xpPluginClasses );
      rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      Metrics.counter( "init", stepname ).inc();
    } catch ( Exception e ) {
      Metrics.counter( "error", stepname ).inc();
      LOG.error( "Error in setup of converting input data into Kettle rows : " + e.getMessage() );
      throw new RuntimeException( "Error in setup of converting input data into Kettle rows", e );
    }
  }

  @ProcessElement
  public void processElement( ProcessContext processContext ) {

    try {

      String inputString = processContext.element();
      inputCounter.inc();

      String[] components;

      Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
      int index = 0;

      boolean hasFieldEnclosure = fileDefinition.getEnclosure() != null;

      if(!Strings.isNullOrEmpty(fileDefinition.getSeparator())) {
        if(hasFieldEnclosure) {
          inputString = inputString.replace(fileDefinition.getEnclosure(), "");
        }

        if(inputString.contains(fileDefinition.getSeparator())) {
          components = inputString.split(fileDefinition.getSeparator());
        } else {
          LOG.error("The given input line does not contain any field separator specified in the file definition.");
          throw new Exception("The given input line does not contain any field separator specified in the file definition.");
        }
      } else if(hasFieldEnclosure) {
        List<String> list = new ArrayList<>(Arrays.asList(inputString.split("'")));
        list.removeIf(e -> e.equals(""));

        components = list.toArray(new String[0]);
      } else {
        int indexDef = 0;
        int arrPos = 0;

        components = new String[fileDefinition.getFieldDefinitions().size()];

        for(FieldDefinition field: fileDefinition.getFieldDefinitions()) {
          if(field.getLength() != -1) {
            components[arrPos] = inputString.substring(indexDef, indexDef + field.getLength()).trim();
            arrPos++;
            indexDef = indexDef + field.getLength();
          } else {
            LOG.error("All the fields must have a length specified when there is not a field separator or enclosure.");
            throw new Exception("All the fields must have a length specified when there is not a field separator or enclosure.");
          }
        }
      }

      for(FieldDefinition field: fileDefinition.getFieldDefinitions()) {
        Object value;

        components[index] = components[index].trim();

        switch (field.getValueMeta().getType()) {
          case ValueMetaInterface.TYPE_STRING:
          case ValueMetaInterface.TYPE_INET:
          case ValueMetaInterface.TYPE_NONE:
          case ValueMetaInterface.TYPE_SERIALIZABLE:
            value = Strings.convert(components[index], String.class);
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            value = Strings.convert(components[index], Integer.class);
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            value = Strings.convert(components[index], Double.class);
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            value = Strings.convert(components[index], Float.class);
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            value = Strings.convert(components[index], Boolean.class);
            break;
          case ValueMetaInterface.TYPE_DATE:
            if(field.getFormatMask() != null) {
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern(field.getFormatMask());
              LocalDate date = LocalDate.parse(components[index], formatter);
              value = java.sql.Date.valueOf(date);
              break;
            } else {
              LOG.error("Date field without a format specified.");
              throw new Exception("Date field without a format specified.");
            }
          case ValueMetaInterface.TYPE_TIMESTAMP:
            if(field.getFormatMask() != null) {
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern(field.getFormatMask());
              LocalDateTime timestamp = LocalDateTime.parse(components[index], formatter);
              value = java.sql.Timestamp.valueOf(timestamp);
              break;
            } else {
              LOG.error("Timestamp field without a format specified.");
              throw new Exception("Timestamp field without a format specified.");
            }
          default:
            value = Strings.convert(components[index], String.class);
        }

        row[index] = value;
        index++;
      }

      // Pass the row to the process context
      //
      processContext.output( new KettleRow( row ) );
      writtenCounter.inc();

    } catch ( Exception e ) {
      Metrics.counter( "error", stepname ).inc();
      LOG.error( "Error converting input data into Kettle rows " + processContext.element() + ", " + e.getMessage() );
      throw new RuntimeException( "Error converting input data into Kettle rows", e );

    }
  }
}
