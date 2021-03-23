package org.kettle.beam.steps.io;

import com.google.cloud.Tuple;
import com.google.cloud.storage.*;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import org.apache.commons.io.FileUtils;
import org.kettle.beam.core.util.DateTime;
import org.kettle.beam.core.util.Strings;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Types;
import java.util.*;

public class BeamOutput extends BaseStep implements StepInterface {

  private File file;
  private String fileName;
  private Boolean isLocalFile;
  private String separator = ",";
  private String enclosure = "\"";
  private String datePattern = "yyyy-MM-dd HH:mm:ss.SSS";
  private Storage storage;
  private Bucket bucket;
  private Blob blob;

  /**
   * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
   * steps.
   *
   * @param stepMeta          The StepMeta object to run.
   * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
   *                          hashtables etc.
   * @param copyNr            The copynumber for this step.
   * @param transMeta         The TransInfo of which the step stepMeta is part of.
   * @param trans             The (running) transformation to obtain information shared among the steps.
   */
  public BeamOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                     Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    BeamOutputMeta meta = (BeamOutputMeta)smi;

    Object[] row = this.getRow();
    if (row==null) {
      setOutputDone();
      return false;
    }

    Map<String, Tuple<Object, Integer>> dataSet = this.getDateSet(row);

    if(this.isLocalFile(meta)){
      this.writeLocalFile(meta, dataSet);
    }else{
      this.writeGSFile(meta, dataSet);
    }

    putRow(getInputRowMeta(), row);
    return true;
  }

  private String getFileName(BeamOutputMeta meta) throws KettleException {
    if(!Strings.isNullOrEmpty(this.fileName)){return this.fileName;}
    String outputLocation = this.getParentVariableSpace().environmentSubstitute(meta.getOutputLocation());
    String filePrefix = this.getParentVariableSpace().environmentSubstitute(meta.getFilePrefix());
    String fileSuffix = this.getParentVariableSpace().environmentSubstitute(meta.getFileSuffix());
    this.fileName = outputLocation;
    if(Strings.isNullOrEmpty(this.fileName)){throw new KettleException("Arquivo não informado.");}
    this.fileName = this.fileName.trim();
    if(!Strings.isNullOrEmpty(filePrefix) && !this.fileName.startsWith(filePrefix)){
      this.fileName = filePrefix.trim() + this.fileName;
    }
    if(!Strings.isNullOrEmpty(fileSuffix) && !this.fileName.endsWith(fileSuffix)){
      this.fileName = this.fileName + fileSuffix.trim();
    }
    return this.fileName;
  }

  private Boolean isLocalFile(BeamOutputMeta meta) throws KettleException {
    if(this.isLocalFile != null){return this.isLocalFile;}
    this.isLocalFile = !this.getFileName(meta).toLowerCase().startsWith("gs://");
    return this.isLocalFile;
  }

  private File getFile(BeamOutputMeta meta) throws KettleException{
    if(this.file != null){return this.file;}
    this.file = new File(this.getFileName(meta));
    return this.file;
  }

  private String toStringRow(Map<String, Tuple<Object, Integer>> dataSet){
    if(dataSet == null){return null;}
    StringBuilder builder = new StringBuilder();
    int count = 0;
    Object value;
    for(Map.Entry<String, Tuple<Object, Integer>> entry : dataSet.entrySet()){
      if(count > 0){builder.append(this.separator);}
      builder.append(this.enclosure);
      value = entry.getValue().x();
      if(value != null){

        if(value instanceof Calendar){
          builder.append(DateTime.toString((Calendar)value, this.datePattern));

        }else if(value instanceof Date) {
          builder.append(DateTime.toString((Date)value, this.datePattern));

        }else if(value instanceof Boolean) {
          builder.append(((Boolean)value) ? "true" : "false");

        }else{
          builder.append(value.toString());
        }
      }
      builder.append(this.enclosure);
      count++;
    }
    return builder.toString();
  }

  private void writeLocalFile(BeamOutputMeta meta, Map<String, Tuple<Object, Integer>> dataSet) throws KettleException{
    if(meta == null || dataSet == null || dataSet.size() == 0){return;}
    String value = this.toStringRow(dataSet) + "\n";
    File file = this.getFile(meta);
    if(file == null){throw new KettleException("Arquivo não acessado.");}
    try {
      FileUtils.writeStringToFile(file, value, true);
    }catch (Exception ex){
      throw new KettleException(ex.getMessage(), ex);
    }
  }

  private void writeGSFile(BeamOutputMeta meta, Map<String, Tuple<Object, Integer>> dataSet) throws KettleException{
    String fileName = this.getFileName(meta);
    String bucketName = fileName.substring(fileName.indexOf("gs://") + 5, fileName.lastIndexOf("/"));
    String shortFileName = fileName.substring(fileName.lastIndexOf("/")+1);
    String value = this.toStringRow(dataSet) + "\n";
    try (CloudStorageFileSystem fs = CloudStorageFileSystem.forBucket(bucketName)) {
      Path path = fs.getPath(shortFileName);
      Files.write(path, value.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }catch (Exception ex){
      throw new KettleException(ex.getMessage(), ex);
    }
  }

  private Map<String, Tuple<Object, Integer>> getDateSet(Object[] row) throws KettleValueException {
    Map<String, Tuple<Object, Integer>> dataSet = new HashMap<>();
    if(row == null){return dataSet;}
    String field;
    Tuple<Object, Integer> tuple;
    int i = 0;
    for(ValueMetaInterface valueMetaInterface : this.getInputRowMeta().getValueMetaList()){
      field = valueMetaInterface.getName().trim();
      tuple = null;
      switch (valueMetaInterface.getType()){
        case ValueMetaInterface.TYPE_STRING: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.VARCHAR) ; break;
        case ValueMetaInterface.TYPE_INTEGER: tuple = Tuple.of(this.getInputRowMeta().getInteger(row, i), Types.INTEGER); break;
        case ValueMetaInterface.TYPE_NUMBER: tuple = Tuple.of(this.getInputRowMeta().getNumber(row, i), Types.NUMERIC); break;
        case ValueMetaInterface.TYPE_BIGNUMBER: tuple = Tuple.of(this.getInputRowMeta().getBigNumber(row, i), Types.NUMERIC); break;
        case ValueMetaInterface.TYPE_BOOLEAN: tuple = Tuple.of(this.getInputRowMeta().getBoolean(row, i), Types.BOOLEAN); break;
        case ValueMetaInterface.TYPE_DATE: tuple = Tuple.of(this.getInputRowMeta().getDate(row, i), Types.DATE); break;
        case ValueMetaInterface.TYPE_TIMESTAMP: tuple = Tuple.of(this.getInputRowMeta().getDate(row, i), Types.TIMESTAMP); break;
        case ValueMetaInterface.TYPE_INET: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.DATALINK); break;
        case ValueMetaInterface.TYPE_NONE: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.BINARY); break;
        case ValueMetaInterface.TYPE_SERIALIZABLE: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.BINARY); break;
      }
      if(tuple != null) {
        dataSet.put(field, tuple);
      }
      i++;
    }
    return dataSet;
  }

}
