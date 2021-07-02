package org.kettle.beam.steps.io;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.metastore.FieldDefinition;
import org.kettle.beam.metastore.FileDefinition;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = BeamConst.STRING_BEAM_OUTPUT_PLUGIN_ID,
  name = "Beam Output",
  description = "Describes a Beam Output",
  image = "beam-output.svg",
  categoryDescription = "Big Data"
)
public class BeamOutputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String OUTPUT_LOCATION = "output_location";
  public static final String FILE_DESCRIPTION_NAME = "file_description_name";
  public static final String FILE_PREFIX = "file_prefix";
  public static final String FILE_SUFFIX = "file_suffix";
  public static final String WINDOWED = "windowed";

  public static final String SEPARATOR = "separator";
  public static final String ENCLOSURE = "enclosure";

  private String outputLocation;
  private String fileDescriptionName;
  private String filePrefix;
  private String fileSuffix;
  private boolean windowed;

  private String separator;
  private String enclosure;
  private List<FieldDefinition> fields;

  public BeamOutputMeta() {
    super();
    fields = new ArrayList<>();
  }

  @Override public void setDefault() {
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {

    return new BeamOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new BeamOutputData();
  }

  @Override public String getDialogClassName() {
    return BeamOutputDialog.class.getName();
  }

  public FileDefinition loadFileDefinition( IMetaStore metaStore) throws KettleStepException {
    if ( StringUtils.isEmpty(fileDescriptionName)) {
      throw new KettleStepException("No file description name provided");
    }
    FileDefinition fileDefinition;
    try {
      MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, metaStore, PentahoDefaults.NAMESPACE );
      fileDefinition = factory.loadElement( fileDescriptionName );
    } catch(Exception e) {
      throw new KettleStepException( "Unable to load file description '"+fileDescriptionName+"' from the metastore", e );
    }

    return fileDefinition;
  }

  @Override public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
          throws KettleStepException {

    if (metaStore!=null) {
      FileDefinition fileDefinition = parseFileDefinition();

      try {
        inputRowMeta.clear();
        inputRowMeta.addRowMeta( fileDefinition.getRowMeta() );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( "Unable to get row layout of file definition '" + fileDefinition.getName() + "'", e );
      }
    }
  }

  public FileDefinition parseFileDefinition() throws KettleStepException {
    FileDefinition fileDefinition = new FileDefinition();
    fileDefinition.setFieldDefinitions(this.getFields());
    fileDefinition.setSeparator(Const.NVL(this.getSeparator(), ""));
    fileDefinition.setEnclosure(Const.NVL(this.getEnclosure(), ""));

    return fileDefinition;
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer(  );

    xml.append( XMLHandler.addTagValue( OUTPUT_LOCATION, outputLocation ) );
    xml.append( XMLHandler.addTagValue( FILE_DESCRIPTION_NAME, fileDescriptionName) );
    xml.append( XMLHandler.addTagValue( FILE_PREFIX, filePrefix) );
    xml.append( XMLHandler.addTagValue( FILE_SUFFIX, fileSuffix) );
    xml.append( XMLHandler.addTagValue( WINDOWED, windowed) );

    xml.append( XMLHandler.addTagValue( SEPARATOR, separator) );
    xml.append( XMLHandler.addTagValue( ENCLOSURE, enclosure) );

    xml.append( XMLHandler.openTag( "fields" ) );
    for ( FieldDefinition field : fields ) {
      xml.append( XMLHandler.openTag( "field" ) );
      xml.append( XMLHandler.addTagValue( "name", field.getName() ) );
      xml.append( XMLHandler.addTagValue( "type", field.getKettleType() ) );
      xml.append( XMLHandler.addTagValue( "length", field.getLength() ) );
      xml.append( XMLHandler.addTagValue( "precision", field.getPrecision() ) );
      xml.append( XMLHandler.addTagValue( "mask", field.getFormatMask() ) );
      xml.append( XMLHandler.closeTag( "field" ) );
    }
    xml.append( XMLHandler.closeTag( "fields" ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {

    outputLocation = XMLHandler.getTagValue( stepnode, OUTPUT_LOCATION );
    fileDescriptionName = XMLHandler.getTagValue( stepnode, FILE_DESCRIPTION_NAME );
    filePrefix = XMLHandler.getTagValue( stepnode, FILE_PREFIX );
    fileSuffix = XMLHandler.getTagValue( stepnode, FILE_SUFFIX );
    windowed = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, WINDOWED) );

    separator = XMLHandler.getTagValue( stepnode, SEPARATOR );
    enclosure = XMLHandler.getTagValue( stepnode, ENCLOSURE );

    Node fieldsNode = XMLHandler.getSubNode( stepnode, "fields" );
    List<Node> fieldNodes = XMLHandler.getNodes( fieldsNode, "field" );
    fields = new ArrayList<>();
    for ( Node fieldNode : fieldNodes ) {
      String name = XMLHandler.getTagValue( fieldNode, "name" );
      String kettleType = XMLHandler.getTagValue( fieldNode, "type" );
      String mask = XMLHandler.getTagValue( fieldNode, "mask" );
      String length = XMLHandler.getTagValue( fieldNode, "length" );
      String precision = XMLHandler.getTagValue( fieldNode, "precision" );
      fields.add( new FieldDefinition( name, kettleType, Integer.parseInt(length), Integer.parseInt(precision), mask ) );
    }
  }

  /**
   * Gets outputLocation
   *
   * @return value of outputLocation
   */
  public String getOutputLocation() {
    return outputLocation;
  }

  /**
   * @param outputLocation The outputLocation to set
   */
  public void setOutputLocation( String outputLocation ) {
    this.outputLocation = outputLocation;
  }

  /**
   * Gets fileDescriptionName
   *
   * @return value of fileDescriptionName
   */
  public String getFileDescriptionName() {
    return fileDescriptionName;
  }

  /**
   * @param fileDescriptionName The fileDescriptionName to set
   */
  public void setFileDescriptionName( String fileDescriptionName ) {
    this.fileDescriptionName = fileDescriptionName;
  }

  /**
   * Gets filePrefix
   *
   * @return value of filePrefix
   */
  public String getFilePrefix() {
    return filePrefix;
  }

  /**
   * @param filePrefix The filePrefix to set
   */
  public void setFilePrefix( String filePrefix ) {
    this.filePrefix = filePrefix;
  }

  /**
   * Gets fileSuffix
   *
   * @return value of fileSuffix
   */
  public String getFileSuffix() {
    return fileSuffix;
  }

  /**
   * @param fileSuffix The fileSuffix to set
   */
  public void setFileSuffix( String fileSuffix ) {
    this.fileSuffix = fileSuffix;
  }

  /**
   * Gets windowed
   *
   * @return value of windowed
   */
  public boolean isWindowed() {
    return windowed;
  }

  /**
   * @param windowed The windowed to set
   */
  public void setWindowed( boolean windowed ) {
    this.windowed = windowed;
  }

  public String getSeparator() {
    return separator;
  }

  public void setSeparator(String separator) {
    this.separator = separator;
  }

  public String getEnclosure() {
    return enclosure;
  }

  public void setEnclosure(String enclosure) {
    this.enclosure = enclosure;
  }

  public List<FieldDefinition> getFields() {
    return fields;
  }

  public void setFields(List<FieldDefinition> fields) {
    this.fields = fields;
  }
}
