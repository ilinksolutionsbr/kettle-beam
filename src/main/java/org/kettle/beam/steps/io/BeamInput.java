package org.kettle.beam.steps.io;

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class BeamInput extends BaseStep implements StepInterface {

  private String fileName;
  private Boolean isLocalFile;
  private File file;

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
  public BeamInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    BeamInputMeta meta = (BeamInputMeta)smi;

    FileDefinition fileDefinition = meta.parseFileDefinition();

    if(this.isLocalFile(meta)){
      this.readLocalFile(meta, fileDefinition);
    }else{
      this.readGSFile(meta, fileDefinition);
    }

    return false;
  }

  private String getFileName(BeamInputMeta meta) throws KettleException {
    if(!Strings.isNullOrEmpty(this.fileName)){return this.fileName;}
    String inputLocation = this.getParentVariableSpace().environmentSubstitute(meta.getInputLocation());
    this.fileName = inputLocation;
    if(Strings.isNullOrEmpty(this.fileName)){throw new KettleException("Arquivo não informado.");}
    this.fileName = this.fileName.trim();
    return this.fileName;
  }

  private Boolean isLocalFile(BeamInputMeta meta) throws KettleException {
    if(this.isLocalFile != null){return this.isLocalFile;}
    this.isLocalFile = !this.getFileName(meta).toLowerCase().startsWith("gs://");
    return this.isLocalFile;
  }

  private File getFile(BeamInputMeta meta) throws KettleException{
    if(this.file != null){return this.file;}
    this.file = new File(this.getFileName(meta));
    return this.file;
  }

  private void readLocalFile(BeamInputMeta meta, FileDefinition fileDefinition) throws KettleException{
    File file = this.getFile(meta);

    if (file == null){throw new KettleException("Arquivo não acessado.");}

    try (Stream<String> stream = Files.lines(file.toPath())) {
      stream.forEach(l -> processLine(l, fileDefinition));

      this.setOutputDone();
    } catch (Exception ex){
      throw new KettleException(ex.getMessage(), ex);
    }
  }

  private void readGSFile(BeamInputMeta meta, FileDefinition fileDefinition) throws KettleException{
    String fileName = this.getFileName(meta);
    if (fileName == null){throw new KettleException("Arquivo não acessado.");}

    String bucketName = fileName.substring(fileName.indexOf("gs://") + 5, fileName.lastIndexOf("/"));
    String shortFileName = fileName.substring(fileName.lastIndexOf("/") + 1);

    try (CloudStorageFileSystem fs = CloudStorageFileSystem.forBucket(bucketName)) {
      Path path = fs.getPath(shortFileName);

      try (Stream<String> stream = Files.lines(path)) {
        stream.forEach(l -> processLine(l, fileDefinition));
      }

      this.setOutputDone();
    }catch (Exception ex){
      throw new KettleException(ex.getMessage(), ex);
    }
  }

  private void processLine(String line, FileDefinition fileDefinition) {
    String[] arr;
    Object[] row = new Object[fileDefinition.getFieldDefinitions().size()];
    int i = 0;

    try {
      boolean hasFieldEnclosure = fileDefinition.getEnclosure() != null;

      if(fileDefinition.getSeparator() != null) {
        if(hasFieldEnclosure) {
          line = line.replace(fileDefinition.getEnclosure(), "");
        }

        if(line.contains(fileDefinition.getSeparator())) {
          arr = line.split(fileDefinition.getSeparator());
        } else {
          log.logError("A linha especificada não possui o separador de campo especificado na definição de arquivo.");
          throw new Exception("A linha especificada não possui o separador de campo especificado na definição de arquivo.");
        }
      } else if(hasFieldEnclosure) {
        List<String> list = new ArrayList<>(Arrays.asList(line.split("'")));
        list.removeIf(e -> e.equals(""));

        arr = list.toArray(new String[0]);
      } else {
        int index = 0;
        int arrPos = 0;

        arr = new String[fileDefinition.getFieldDefinitions().size()];

        for(FieldDefinition field: fileDefinition.getFieldDefinitions()) {
          if(field.getLength() != -1) {
            arr[arrPos] = line.substring(index, index + field.getLength()).trim();
            arrPos++;
            index = index + field.getLength();
          } else {
            log.logError("Todos os campos devem possuir tamanho definido quando não existir separador ou fechador de campo");
            throw new Exception("Todos os campos devem possuir tamanho definido quando não existir separador ou fechador de campo");
          }
        }
      }

      for(FieldDefinition field: fileDefinition.getFieldDefinitions()) {
      Object value;

        switch (field.getValueMeta().getType()) {
          case ValueMetaInterface.TYPE_STRING:
          case ValueMetaInterface.TYPE_INET:
          case ValueMetaInterface.TYPE_NONE:
          case ValueMetaInterface.TYPE_SERIALIZABLE:
            value = Strings.convert(arr[i], String.class);
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            value = Strings.convert(arr[i], Integer.class);
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            value = Strings.convert(arr[i], Double.class);
            break;
          case ValueMetaInterface.TYPE_BIGNUMBER:
            value = Strings.convert(arr[i], Float.class);
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            value = Strings.convert(arr[i], Boolean.class);
            break;
          case ValueMetaInterface.TYPE_DATE:
            if(field.getFormatMask() != null) {
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern(field.getFormatMask());
              LocalDate date = LocalDate.parse(arr[i], formatter);
              value = java.sql.Date.valueOf(date);
              break;
            } else {
              logError("Campo de data sem formato especificado");
              throw new Exception("Campo de data sem formato especificado");
            }
          case ValueMetaInterface.TYPE_TIMESTAMP:
            if(field.getFormatMask() != null) {
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern(field.getFormatMask());
              LocalDateTime timestamp = LocalDateTime.parse(arr[i], formatter);
              value = Timestamp.valueOf(timestamp);
              break;
            } else {
              logError("Campo de timestamp sem formato especificado");
              throw new Exception("Campo de timestamp sem formato especificado");
            }
          default:
            value = Strings.convert(arr[i], String.class);
        }

        row[i] = value;
        i++;
      }

      this.putRow(fileDefinition.getRowMeta(), row);
    } catch (Exception e) {
      logError(e.getMessage(), e);
    }
  }
}