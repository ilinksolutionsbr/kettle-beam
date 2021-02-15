package org.kettle.beam.steps.io;

import org.kettle.beam.core.util.Strings;
import org.kettle.beam.metastore.FileDefinition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.File;

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
    FileDefinition fileDefinition = meta.loadFileDefinition(metaStore);

    if(this.isLocalFile(meta)){
      this.readLocalFile(meta, fileDefinition);
    }else{
      this.readGSFile(meta, fileDefinition);
    }

    return true;
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
    if(file == null){throw new KettleException("Arquivo não acessado.");}
    try {

    }catch (Exception ex){
      throw new KettleException(ex.getMessage(), ex);
    }
  }

  private void readGSFile(BeamInputMeta meta, FileDefinition fileDefinition) throws KettleException{

  }

}
