package org.kettle.beam.metastore;

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.metastore.persist.MetaStoreAttribute;

public class FieldDefinition {

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String kettleType;

  @MetaStoreAttribute
  private int length;

  @MetaStoreAttribute
  private int precision;

  @MetaStoreAttribute
  private String formatMask;

  public FieldDefinition( ) {
  }

  public FieldDefinition( String name, String kettleType, int length, int precision ) {
    this.name = name;
    this.kettleType = kettleType;
    this.length = length;
    this.precision = precision;
  }

  public FieldDefinition( String name, String kettleType, int length, int precision, String formatMask ) {
    this.name = name;
    this.kettleType = kettleType;
    this.length = length;
    this.precision = precision;
    this.formatMask = formatMask;
  }

  public ValueMetaInterface getValueMeta() throws KettlePluginException {
    int type = ValueMetaFactory.getIdForValueMeta( kettleType );
    ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( name, type, length, precision );
    valueMeta.setConversionMask( formatMask );
    return valueMeta;
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
   * Gets kettleType
   *
   * @return value of kettleType
   */
  public String getKettleType() {
    return kettleType;
  }

  /**
   * @param kettleType The kettleType to set
   */
  public void setKettleType( String kettleType ) {
    this.kettleType = kettleType;
  }

  /**
   * Gets length
   *
   * @return value of length
   */
  public int getLength() {
    return length;
  }

  /**
   * @param length The length to set
   */
  public void setLength( int length ) {
    this.length = length;
  }

  /**
   * Gets precision
   *
   * @return value of precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @param precision The precision to set
   */
  public void setPrecision( int precision ) {
    this.precision = precision;
  }

  /**
   * Gets formatMask
   *
   * @return value of formatMask
   */
  public String getFormatMask() {
    return formatMask;
  }

  /**
   * @param formatMask The formatMask to set
   */
  public void setFormatMask( String formatMask ) {
    this.formatMask = formatMask;
  }
}
