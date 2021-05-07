package org.kettle.beam.steps.firestore;

import com.google.api.client.util.Strings;
import com.google.cloud.datastore.*;

import java.util.HashMap;
import java.util.Map;

import org.kettle.beam.core.fn.FirestoreEntityToKettleRowFn;
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
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;

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

      String queryString = meta.getQuery();
      String kind = meta.getKind();

    Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

    try {
        if(Strings.isNullOrEmpty(queryString)) {
            if(!Strings.isNullOrEmpty(kind)) {

                Query<Entity> query = Query.newEntityQueryBuilder()
                        .setKind(kind)
                        .build();

                QueryResults<Entity> results = datastore.run(query);

                processQuery(results, outputRowMeta);
            } else {
                throw new KettlePluginException("O nome da kind deve ser informado ao buscar todas as entidades");
            }
        } else {
            GqlQuery<?> gqlQuery = GqlQuery.newGqlQueryBuilder(queryString).setAllowLiteral(true).build();

            QueryResults<?> results = datastore.run(gqlQuery);

            processQuery(results, outputRowMeta);
        }
    } catch (Exception ex) {
        this.log.logError("Google Firestore Input", ex);
        this.setOutputDone();
        throw new KettleException(ex.getMessage(), ex);
    }

    this.setOutputDone();

    return false;
  }

    private void processQuery(QueryResults<?> results, RowMeta outputRowMeta) throws KettleException {
        Map<String, Value<?>> properties;

        if(results.hasNext()) {
            Class<?> clazz = results.getResultClass();
            while (results.hasNext()) {
                if(clazz.equals(Entity.class)) {
                    Entity result = (Entity) results.next();
                    properties = result.getProperties();
                } else if (clazz.equals(ProjectionEntity.class)) {
                    ProjectionEntity result = (ProjectionEntity) results.next();
                    properties = result.getProperties();
                } else {
                    throw new KettleException("O tipo de dado retornado nao e valido para os padroes da aplicacao");
                }

                flush(outputRowMeta, properties);
            }
        } else {
            throw new KettleException("A kind informada nao existe ou nao possui nenhum registro para obter os campos");
        }
    }

    private void flush(RowMeta outputRowMeta, Map<String, Value<?>> properties) {
        try{
            if(outputRowMeta.size() == 0){return;}
            Object[] newRow = new Object[outputRowMeta.size()];
            int index = 0;

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

