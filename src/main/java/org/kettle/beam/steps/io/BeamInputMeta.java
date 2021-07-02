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
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = BeamConst.STRING_BEAM_INPUT_PLUGIN_ID,
  name = "Beam Input",
  description = "Describes a Beam Input",
  image = "beam-input.svg",
  categoryDescription = "Big Data"
)
public class BeamInputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String INPUT_LOCATION = "input_location";

  public static final String SEPARATOR = "separator";
  public static final String ENCLOSURE = "enclosure";

  private String inputLocation;
  private String fileDescriptionName;

  private String name;
  private String description;
  private String separator;
  private String enclosure;
  private List<FieldDefinition> fields;

  public BeamInputMeta() {
    super();
    fields = new ArrayList<>();
  }

  @Override public void setDefault() {
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new BeamInput( stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  @Override public StepDataInterface getStepData() {
    return new BeamInputData();
  }

  @Override public String getDialogClassName() {
    return BeamInputDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
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

  public FileDefinition loadFileDefinition(IMetaStore metaStore) throws KettleStepException {
    if (StringUtils.isEmpty(fileDescriptionName)) {
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

  public FileDefinition parseFileDefinition() throws KettleStepException {
    FileDefinition fileDefinition = new FileDefinition();
    fileDefinition.setFieldDefinitions(this.getFields());
    fileDefinition.setSeparator(Const.NVL(this.getSeparator(), ""));
    fileDefinition.setEnclosure(Const.NVL(this.getEnclosure(), ""));

    return fileDefinition;
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer(  );

    xml.append( XMLHandler.addTagValue( INPUT_LOCATION, inputLocation ) );

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

    inputLocation = XMLHandler.getTagValue( stepnode, INPUT_LOCATION );

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
   * Gets inputLocation
   *
   * @return value of inputLocation
   */
  public String getInputLocation() {
    return inputLocation;
  }

  /**
   * @param inputLocation The inputLocation to set
   */
  public void setInputLocation( String inputLocation ) {
    this.inputLocation = inputLocation;
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

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
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
