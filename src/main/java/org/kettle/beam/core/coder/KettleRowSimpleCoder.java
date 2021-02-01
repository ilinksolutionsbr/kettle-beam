package org.kettle.beam.core.coder;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class KettleRowSimpleCoder extends AtomicCoder<KettleRow> {

  @Override public void encode( KettleRow value, OutputStream outStream ) throws CoderException, IOException {
    Object[] row = value.getRow();
    ObjectOutputStream out = new ObjectOutputStream( outStream );
    this.ensureDataTypes(row);
    out.writeObject(row);
    out.flush();
  }

  @Override public KettleRow decode( InputStream inStream ) throws CoderException, IOException {
    ObjectInputStream in = new ObjectInputStream( inStream );
    Object value;
    Object[] row = null;
    try {
      value = in.readObject();
      row = (Object[]) value;
      this.ensureDataTypes(row);
    }catch (Exception ex){}
    return row != null && row.length > 0 ? new KettleRow(row) : new KettleRow(null);
  }

  @Override public void verifyDeterministic() throws NonDeterministicException {
    // Sure
  }

  private void ensureDataTypes(Object[] row) throws IOException {
    if(row == null){return;}
    Object value;

    for(int i = 0; i < row.length; i++){
      value = row[i];
      if(value != null){

        if(value instanceof Calendar){
          row[i] = ((Calendar)value).getTime();

        }else if(value instanceof java.sql.Date) {
          row[i] = new Date(((java.sql.Date) value).getTime());

        }else if(value instanceof BigDecimal) {
          row[i] = ((BigDecimal)value).doubleValue();

        }else if(value instanceof Boolean
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Double
                || value instanceof Float
                || value instanceof Date
                || value instanceof String) {
          row[i] = value;

        }else{
          row[i] = value.toString();

        }

      }
    }

  }


}
