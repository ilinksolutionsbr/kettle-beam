package org.kettle.beam.steps.bq;

import com.google.cloud.bigquery.*;
import com.google.common.base.Strings;
import org.kettle.beam.steps.database.BeamDatabaseConnectorHelper;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;


public class BeamBQInput extends BaseStep implements StepInterface {


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
    public BeamBQInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        BeamBQInputMeta meta = (BeamBQInputMeta)smi;
        BeamBQInputData data = (BeamBQInputData)sdi;

        RowMeta outputRowMeta = new RowMeta();
        for(BQField field : meta.getFields()){
            outputRowMeta.addValueMeta(this.createValueMeta(field));
        }

        StringBuilder queryBuilder = new StringBuilder();
        if(!Strings.isNullOrEmpty(meta.getQuery())){
            queryBuilder.append(meta.getQuery());
        }else{
            int i = 0;
            queryBuilder.append("SELECT ");
            for(BQField field : meta.getFields()){
                if(i > 0){queryBuilder.append(", ");}
                queryBuilder.append("[" + field.getName() + "]");
                i++;
            }
            queryBuilder.append(" FROM ");
            queryBuilder.append("`");
            queryBuilder.append(meta.getProjectId());
            queryBuilder.append(".");
            queryBuilder.append(meta.getDatasetId());
            queryBuilder.append(".");
            queryBuilder.append(meta.getTableId());
            queryBuilder.append("`");
        }
        String query = queryBuilder.toString();
        query = this.setParameters(query);
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                this.flush(outputRowMeta, row);
            }
        }catch (Exception ex){
            this.log.logError("Google BigQuery Input", ex);
            this.setOutputDone();
            throw new KettleException(ex.getMessage(), ex);
        }

        this.setOutputDone();

        return false;
    }

    private void flush(RowMeta outputRowMeta, FieldValueList row) {
        try{
            if(outputRowMeta.size() == 0){return;}
            Object[] newRow = new Object[outputRowMeta.size()];
            int index = 0;
            for (FieldValue value : row) {
                newRow[index] = value.getValue();
                index++;
            }
            this.putRow(outputRowMeta, newRow);
            if (isRowLevel()) {
                logRowlevel("Google BigQuery Input", outputRowMeta.getString(newRow));
            }
        }catch (Exception ex){
            this.log.logError("Google BigQuery Input", ex);
        }
    }

    private ValueMetaInterface createValueMeta(BQField field){
        if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_BIG_NUMBER)){
            return new ValueMetaBigNumber(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_BINARY)) {
            return new ValueMetaBinary(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_BOOLEAN)) {
            return new ValueMetaBoolean(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_DATE)) {
            return new ValueMetaDate(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_INTEGER)) {
            return new ValueMetaInteger(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_INTERNET_ADDRESS)) {
            return new ValueMetaInternetAddress(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_NUMBER)) {
            return new ValueMetaNumber(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_STRING)) {
            return new ValueMetaString(field.getName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_TIMESTAMP)) {
            return new ValueMetaTimestamp(field.getName());

        } else{
            return new ValueMetaString(field.getName());
        }
    }

    private String setParameters(String query){
        int startIndex = query.indexOf("${");
        int finishIndex = query.indexOf("}");
        String value;
        String variable;
        String queryPart1;
        String queryPart2;
        if(startIndex >= 0 && startIndex < finishIndex){
            queryPart1 = query.substring(0, startIndex);
            queryPart2 = query.substring(finishIndex + 1);
            variable = query.substring(startIndex + 2, finishIndex);
            value = this.getParentVariableSpace().environmentSubstitute("${" + variable + "}");
            query = queryPart1 + value + queryPart2;
            return this.setParameters(query);
        }else{
            return query;
        }
    }

}
