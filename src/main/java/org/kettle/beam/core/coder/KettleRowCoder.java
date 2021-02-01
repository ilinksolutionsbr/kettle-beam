package org.kettle.beam.core.coder;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.kettle.beam.core.KettleRow;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class KettleRowCoder extends AtomicCoder<KettleRow> {

  @Override public void encode( KettleRow value, OutputStream outStream ) throws CoderException, IOException {

    Object[] row = value.getRow();
    ObjectOutputStream out = new ObjectOutputStream( outStream );

    // Length
    //
    if ( row == null ) {
      out.writeInt( -1 );
      return; // all done
    } else {
      out.writeInt( row.length );
    }

    // The values
    //
    for ( int i = 0; i < row.length; i++ ) {
      try {
        Object object = row[i];
        // Null?
        //
        out.writeBoolean(object == null || (object instanceof byte[]));

        if (object != null && !(object instanceof byte[])) {
          // Type?
          //
          int objectType = getObjectType(object);
          out.writeInt(objectType);

          // The object itself
          //
          write(out, objectType, object);
        }
      }catch (Exception ex){
        throw ex;
      }
    }
    out.flush();

  }


  @Override public KettleRow decode( InputStream inStream ) throws CoderException, IOException {

    ObjectInputStream in = new ObjectInputStream( inStream );
    Object[] row = null;

    int length = in.readInt();
    if ( length < 0 ) {
      return new KettleRow( row );
    }
    row = new Object[ length ];
    for ( int i = 0; i < length; i++ ) {
      try{
        // Null?
        boolean isNull = in.readBoolean();
        if ( !isNull ) {
          int objectType = in.readInt();
          Object object = read( in, objectType );
          row[i] = object;
        }
      }catch (Exception ex){
        throw ex;
      }
    }

    return new KettleRow(row);

  }

  @Override public void verifyDeterministic() throws NonDeterministicException {
    // Sure
  }



  private void write( ObjectOutputStream out, int objectType, Object object ) throws IOException {
    switch ( objectType ) {
      case ValueMetaInterface.TYPE_STRING: {
        String string = (String) object;
        out.writeUTF( string );
      }
      break;
      case ValueMetaInterface.TYPE_INTEGER: {
        if(object == null) {
          out.writeLong((Long)object);

        }else if ( object instanceof Byte ) {
          out.writeByte((Byte)object);

        }else if ( object instanceof Short ) {
          out.writeShort((Short)object);

        }else if ( object instanceof Integer ) {
          out.writeInt((Integer)object);

        }else if ( object instanceof Long ) {
          out.writeLong((Long)object);

        }else {
          out.writeLong(Long.parseLong(object.toString()));
        }
      }
      break;
      case ValueMetaInterface.TYPE_DATE: {
        if(object == null) {
          out.writeLong((Long)object);

        }else if ( object instanceof Date ) {
          out.writeLong(( (Date) object ).getTime());

        }else if ( object instanceof Calendar ) {
          out.writeLong(( (Calendar) object ).getTime().getTime());

        }else {
          out.writeLong(Long.parseLong(object.toString()));
        }
      }
      break;
      case ValueMetaInterface.TYPE_BOOLEAN: {
        if(object == null) {
          out.writeBoolean(false);

        }else if ( object instanceof Boolean ) {
          out.writeBoolean((Boolean) object);

        }else if ( object instanceof Byte ) {
          out.writeBoolean(((Byte)object) > 0);

        }else if ( object instanceof Short ) {
          out.writeBoolean(((Short)object) > 0);

        }else if ( object instanceof Integer ) {
          out.writeBoolean(((Integer)object) > 0);

        }else if ( object instanceof Long ) {
          out.writeBoolean(((Long)object) > 0);

        }else {
          out.writeBoolean(Boolean.parseBoolean(object.toString()));
        }

      }
      break;
      case ValueMetaInterface.TYPE_NUMBER: {
        if(object == null) {
          out.writeDouble((Double) object);

        }else if ( object instanceof Double ) {
          out.writeDouble((Double)object);

        }else if ( object instanceof Float ) {
          out.writeFloat((Float) object);

        }else {
          out.writeLong(Long.parseLong(object.toString()));
        }
      }
      break;
      case ValueMetaInterface.TYPE_BIGNUMBER: {
        out.writeDouble( ((BigDecimal) object).doubleValue() );
      }
      break;
      default:
        throw new IOException( "Data type not supported yet: " + objectType + " - " + object.toString() );
    }
  }

  private Object read( ObjectInputStream in, int objectType ) throws IOException {
    switch ( objectType ) {
      case ValueMetaInterface.TYPE_STRING: {
        String string = in.readUTF();
        return string;
      }

      case ValueMetaInterface.TYPE_INTEGER: {
        Object result = null;
        try {result = in.readLong();}catch (Exception ex){}
        if(result != null){return result;}
        try {result = in.readInt();}catch (Exception ex){}
        if(result != null){return result;}
        try {result = in.readShort();}catch (Exception ex){}
        if(result != null){return result;}
        try {result = in.readByte();}catch (Exception ex){}
        if(result != null){return result;}
        return null;
      }

      case ValueMetaInterface.TYPE_DATE: {
        Long lng = in.readLong();
        return new Date(lng);
      }

      case ValueMetaInterface.TYPE_BOOLEAN: {
        boolean b = in.readBoolean();
        return b;
      }

      case ValueMetaInterface.TYPE_NUMBER: {
        Object result = null;
        try {result = in.readDouble();}catch (Exception ex){}
        if(result != null){return result;}
        try {result = in.readFloat();}catch (Exception ex){}
        if(result != null){return result;}
        return null;
      }

      case ValueMetaInterface.TYPE_BIGNUMBER: {
        return new BigDecimal(in.readDouble());
      }

      case ValueMetaInterface.TYPE_TIMESTAMP: {
        Long timestamp = in.readLong();
        return timestamp;
      }

      default:
        try {
          return in.readObject();
        }catch (Exception ex){
          throw new IOException( "Data type not supported yet: " + objectType, ex);
        }

    }
  }


  private int getObjectType( Object object ) throws CoderException {
    if ( object instanceof String ) {
      return ValueMetaInterface.TYPE_STRING;
    }
    if ( object instanceof Byte ) {
      return ValueMetaInterface.TYPE_INTEGER;
    }
    if ( object instanceof Short ) {
      return ValueMetaInterface.TYPE_INTEGER;
    }
    if ( object instanceof Integer ) {
      return ValueMetaInterface.TYPE_INTEGER;
    }
    if ( object instanceof Long ) {
      return ValueMetaInterface.TYPE_INTEGER;
    }
    if ( object instanceof Date ) {
      return ValueMetaInterface.TYPE_DATE;
    }
    if ( object instanceof Calendar) {
      return ValueMetaInterface.TYPE_DATE;
    }
    if ( object instanceof Timestamp ) {
      return ValueMetaInterface.TYPE_TIMESTAMP;
    }
    if ( object instanceof Boolean ) {
      return ValueMetaInterface.TYPE_BOOLEAN;
    }
    if ( object instanceof Double) {
      return ValueMetaInterface.TYPE_NUMBER;
    }
    if ( object instanceof Float) {
      return ValueMetaInterface.TYPE_NUMBER;
    }
    if ( object instanceof BigDecimal ) {
      return ValueMetaInterface.TYPE_BIGNUMBER;
    }
    throw new CoderException( "Data type for object class "+object.getClass().getName()+" isn't supported yet" );
  }


}
