package org.kettle.beam.steps.bq;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.steps.database.BeamDatabaseConnectorHelper;

public class BQField {

  public static String BEAM_DATATYPE_BIG_NUMBER = "BigNumber";
  public static String BEAM_DATATYPE_BINARY = "Binary";
  public static String BEAM_DATATYPE_BOOLEAN = "Boolean";
  public static String BEAM_DATATYPE_DATE = "Date";
  public static String BEAM_DATATYPE_INTEGER = "Integer";
  public static String BEAM_DATATYPE_INTERNET_ADDRESS = "Internet Address";
  public static String BEAM_DATATYPE_NUMBER = "Number";
  public static String BEAM_DATATYPE_STRING = "String";
  public static String BEAM_DATATYPE_TIMESTAMP = "Timestamp";


  private String name;
  private String newName;
  private String kettleType;

  public BQField() {
  }

  public BQField( String name, String newName, String kettleType ) {
    this.name = name;
    this.newName = newName;
    this.kettleType = kettleType;
  }

  public String getNewNameOrName() {
    if ( StringUtils.isNotEmpty(newName)) {
      return newName;
    } else {
      return name;
    }
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets newName
   *
   * @return value of newName
   */
  public String getNewName() {
    return newName;
  }

  /**
   * @param newName The newName to set
   */
  public void setNewName( String newName ) {
    this.newName = newName;
  }

  /**
   * Gets kettleType
   *
   * @return value of kettleType
   */
  public String getKettleType() {
    if(Strings.isNullOrEmpty(kettleType)){
      kettleType = BQField.BEAM_DATATYPE_STRING;
    }
    return kettleType;
  }

  /**
   * @param kettleType The kettleType to set
   */
  public void setKettleType( String kettleType ) {
    this.kettleType = kettleType;
  }
}
