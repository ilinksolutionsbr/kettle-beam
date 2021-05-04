package org.kettle.beam.steps.firestore;

import com.google.cloud.datastore.*;

import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 *
 * @author Thiago Teodoro Rodrigues <thiago.rodrigues@callink.com.br>
 */
public class BeamFirestoreInput extends BaseStep implements StepInterface {


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

  public static final String TYPE_STRING = "STRING";

  public BeamFirestoreInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {

      super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    BeamFirestoreInputMeta meta = (BeamFirestoreInputMeta)smi;

      RowMeta outputRowMeta = new RowMeta();
      for(FirestoreField field : meta.getFields()){
          outputRowMeta.addValueMeta(this.createValueMeta(field));
      }

    // The kind for the new entity
    if(meta.getKind() != null){

        String kind = meta.getKind();

        if(meta.getFields().isEmpty()) {

        } else {
            // Obtendo instancia de operações com o Datastore.
            Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

            Query<Entity> query = Query.newEntityQueryBuilder()
                    .setKind(kind)
                    .setLimit(10)
                    .build();

            QueryResults<Entity> results = datastore.run(query);

            try {
                if(results.hasNext()) {
                    while (results.hasNext()) {
                        results.forEachRemaining(projectionEntity -> {
                            this.flush(outputRowMeta, projectionEntity);
                        });
                    }
                } else {
                    throw new KettlePluginException("A kind informada não existe ou não possui nenhum registro");
                }
            } catch (Exception ex) {
                this.log.logError("Google Firestore Input", ex);
                this.setOutputDone();
                throw new KettleException(ex.getMessage(), ex);
            }
        }

        this.setOutputDone();

        return false;
    } else {
        log.logError("Kind vazia!");
        return false;
    }
  }

    private void flush(RowMeta outputRowMeta, Entity entity) {
        try{
            if(outputRowMeta.size() == 0){return;}
            Object[] newRow = new Object[outputRowMeta.size()];
            int index = 0;

            Map<String, Value<?>> properties = entity.getProperties();

            for(Map.Entry<String, Value<?>> entry: properties.entrySet()) {
                String type = entry.getValue().getType().name();
                Object value = entry.getValue().get();

                if(value == null) {
                    newRow[index] = null;
                } else if(type.equals(TYPE_STRING)) {
                    newRow[index] = value;
                } else {
                    Class<?> clazz = outputRowMeta.getValueMetaList().get(index).getNativeDataTypeClass();
                    String stringValue = String.valueOf(value);
                    newRow[index] = org.kettle.beam.core.util.Strings.convert(stringValue, clazz);
                }
                index++;
            }
            this.putRow(outputRowMeta, newRow);
            if (isRowLevel()) {
                logRowlevel("Google Firestore Input", outputRowMeta.getString(newRow));
            }
        }catch (Exception ex){
            this.log.logError("Google Firestore Input", ex);
        }
    }

    private ValueMetaInterface createValueMeta(FirestoreField field){
        if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_BIG_NUMBER)){
            return new ValueMetaBigNumber(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_BINARY)) {
            return new ValueMetaBinary(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_BOOLEAN)) {
            return new ValueMetaBoolean(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_DATE)) {
            return new ValueMetaDate(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_INTEGER)) {
            return new ValueMetaInteger(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_INTERNET_ADDRESS)) {
            return new ValueMetaInternetAddress(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_NUMBER)) {
            return new ValueMetaNumber(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_STRING)) {
            return new ValueMetaString(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(FirestoreField.BEAM_DATATYPE_TIMESTAMP)) {
            return new ValueMetaTimestamp(field.getName());

        } else{
            return new ValueMetaString(field.getName());
        }
    }
}

