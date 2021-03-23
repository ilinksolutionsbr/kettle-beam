package org.kettle.beam.steps.bq;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.*;
import com.google.common.base.Strings;
import org.kettle.beam.steps.database.BeamDatabaseConnectorHelper;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeamBQOutput extends BaseStep implements StepInterface {

    private boolean checkEmpty = false;
    private boolean tableCreated = false;
    private boolean tableTruncated = false;

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
    public BeamBQOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        try {
            BeamBQOutputMeta meta = (BeamBQOutputMeta)smi;
            BeamBQOutputData data = (BeamBQOutputData)sdi;
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            String query;

            Object[] row = this.getRow();
            if (row == null) {
                this.setOutputDone();
                return false;
            }

            //Cria a tabela
            if(!this.tableCreated && meta.isCreatingIfNeeded()){
                this.createTable(bigquery, meta);
                this.tableCreated = true;
            }

            //Trunca a Tabela
            if(!this.tableTruncated && meta.isTruncatingTable()) {
                this.truncateTable(bigquery, meta);
                this.tableTruncated = true;
            }

            //Lança erro se não estiver vazia
            if(!this.checkEmpty && meta.isFailingIfNotEmpty() && !this.isTableEmpty(bigquery, meta)) {
                throw new Exception("A tabela não está vazia.");
            }else{
                this.checkEmpty = true;
            }


            //Prepara e executa a query
            Map<String, Tuple<Object, Integer>> dataSet = this.getDateSet(row);
            query = meta.getQuery();
            if(Strings.isNullOrEmpty(query)){
                query = this.createInsertQuery(meta, dataSet);
            }
            query = this.setParameters(query, dataSet);
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            bigquery.query(queryConfig).getTotalRows();
            this.setOutputDone();

            return true;

        }catch (Exception ex){
            this.log.logError("Google BigQuery Output", ex);
            this.setOutputDone();
            throw new KettleException(ex.getMessage(), ex);
        }
    }

    private void createTable(BigQuery bigquery, BeamBQOutputMeta meta){
        try {
            List<ValueMetaInterface> valueMetaList = this.getTransMeta().getPrevStepFields(this.getStepMeta()).getValueMetaList();
            if(valueMetaList.size() == 0){return;}
            List<Field> fields = new ArrayList<>();
            for(ValueMetaInterface valueMetaInterface : valueMetaList){
                fields.add(Field.of(valueMetaInterface.getName(), this.getType(valueMetaInterface)));
            }
            Schema schema = Schema.of(fields);
            TableId tableId = TableId.of(meta.getDatasetId(), meta.getTableId());
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            bigquery.create(tableInfo);

        } catch( Exception ex ) {
            log.logDebug ("Tabela existente, ignorando a criação da tabela", ex);
        }

    }
    private LegacySQLTypeName getType(ValueMetaInterface valueMetaInterface){
        LegacySQLTypeName type;
        switch (valueMetaInterface.getType()){
            case ValueMetaInterface.TYPE_STRING: type = LegacySQLTypeName.STRING; break;
            case ValueMetaInterface.TYPE_INTEGER: type = LegacySQLTypeName.INTEGER; break;
            case ValueMetaInterface.TYPE_NUMBER: type = LegacySQLTypeName.NUMERIC; break;
            case ValueMetaInterface.TYPE_BIGNUMBER: type = LegacySQLTypeName.FLOAT; break;
            case ValueMetaInterface.TYPE_BOOLEAN: type = LegacySQLTypeName.BOOLEAN; break;
            case ValueMetaInterface.TYPE_DATE: type = LegacySQLTypeName.DATETIME; break;
            case ValueMetaInterface.TYPE_TIMESTAMP: type = LegacySQLTypeName.TIMESTAMP; break;
            case ValueMetaInterface.TYPE_INET: type = LegacySQLTypeName.STRING; break;
            case ValueMetaInterface.TYPE_NONE: type = LegacySQLTypeName.STRING; break;
            case ValueMetaInterface.TYPE_SERIALIZABLE: type = LegacySQLTypeName.STRING; break;
            default: type = LegacySQLTypeName.STRING;
        }
        return type;
    }

    private void truncateTable(BigQuery bigquery, BeamBQOutputMeta meta) throws Exception{
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("TRUNCATE TABLE ");
        queryBuilder.append("`");
        queryBuilder.append(meta.getProjectId());
        queryBuilder.append(".");
        queryBuilder.append(meta.getDatasetId());
        queryBuilder.append(".");
        queryBuilder.append(meta.getTableId());
        queryBuilder.append("`");
        String query = queryBuilder.toString();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        try {
            bigquery.query(queryConfig).getTotalRows();
        }catch (Exception ex){
            this.log.logError("Google BigQuery Output", ex);
            throw ex;
        }
    }

    private boolean isTableEmpty(BigQuery bigquery, BeamBQOutputMeta meta){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT * FROM ");
        queryBuilder.append("`");
        queryBuilder.append(meta.getProjectId());
        queryBuilder.append(".");
        queryBuilder.append(meta.getDatasetId());
        queryBuilder.append(".");
        queryBuilder.append(meta.getTableId());
        queryBuilder.append("`");
        queryBuilder.append(" LIMIT 1");
        String query = queryBuilder.toString();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        boolean isEmpty = true;
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                isEmpty = false;
                break;
            }
        }catch (Exception ex){
            this.log.logError("Google BigQuery Output", ex);
        }
        return isEmpty;
    }

    private Map<String, Tuple<Object, Integer>> getDateSet(Object[] row) throws KettleValueException {
        Map<String, Tuple<Object, Integer>> dataSet = new HashMap<>();
        if(row == null){return dataSet;}
        String field;
        Tuple<Object, Integer> tuple;
        int i = 0;
        for(ValueMetaInterface valueMetaInterface : this.getInputRowMeta().getValueMetaList()){
            field = valueMetaInterface.getName().trim();
            tuple = null;
            switch (valueMetaInterface.getType()){
                case ValueMetaInterface.TYPE_STRING: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.VARCHAR) ; break;
                case ValueMetaInterface.TYPE_INTEGER: tuple = Tuple.of(this.getInputRowMeta().getInteger(row, i), Types.INTEGER); break;
                case ValueMetaInterface.TYPE_NUMBER: tuple = Tuple.of(this.getInputRowMeta().getNumber(row, i), Types.NUMERIC); break;
                case ValueMetaInterface.TYPE_BIGNUMBER: tuple = Tuple.of(this.getInputRowMeta().getBigNumber(row, i), Types.NUMERIC); break;
                case ValueMetaInterface.TYPE_BOOLEAN: tuple = Tuple.of(this.getInputRowMeta().getBoolean(row, i), Types.BOOLEAN); break;
                case ValueMetaInterface.TYPE_DATE: tuple = Tuple.of(this.getInputRowMeta().getDate(row, i), Types.DATE); break;
                case ValueMetaInterface.TYPE_TIMESTAMP: tuple = Tuple.of(this.getInputRowMeta().getDate(row, i), Types.TIMESTAMP); break;
                case ValueMetaInterface.TYPE_INET: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.DATALINK); break;
                case ValueMetaInterface.TYPE_NONE: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.BINARY); break;
                case ValueMetaInterface.TYPE_SERIALIZABLE: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.BINARY); break;
            }
            if(tuple != null) {
                dataSet.put(field, tuple);
            }
            i++;
        }
        return dataSet;
    }

    private String setParameters(String query, Map<String, Tuple<Object, Integer>> dataSet){
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
            value = this.getValue(variable, dataSet);
            query = queryPart1 + value + queryPart2;
            return this.setParameters(query, dataSet);
        }else{
            return query;
        }
    }

    private String getValue(String variable, Map<String, Tuple<Object, Integer>> dataSet){
        String strValue = "";
        Tuple<Object, Integer> tuple;
        if(dataSet != null && dataSet.containsKey(variable)){
            tuple = dataSet.get(variable);
            if(tuple.x() != null) {
                switch (tuple.y()) {
                    case Types.INTEGER:
                    case Types.NUMERIC:
                    case Types.BIGINT:
                    case Types.TIMESTAMP:
                    case Types.BOOLEAN: strValue = tuple.x().toString(); break;
                    case Types.DATE:
                    case Types.DATALINK:
                    case Types.BINARY:
                    case Types.VARCHAR: strValue ="'" + tuple.x().toString() + "'"; break;
                }
            }else{
                strValue = "null";
            }

        }else{
            strValue = this.getParentVariableSpace().environmentSubstitute("${" + variable + "}");
        }
        return strValue;
    }


    private String createInsertQuery(BeamBQOutputMeta meta, Map<String, Tuple<Object, Integer>> dataSet){
        StringBuilder queryBuilder = new StringBuilder();
        int i;

        queryBuilder.append("INSERT INTO ");
        queryBuilder.append("`");
        queryBuilder.append(meta.getProjectId());
        queryBuilder.append(".");
        queryBuilder.append(meta.getDatasetId());
        queryBuilder.append(".");
        queryBuilder.append(meta.getTableId());
        queryBuilder.append("`");
        queryBuilder.append("(");

        i = 0;
        for(Map.Entry<String, Tuple<Object, Integer>> entry : dataSet.entrySet()){
            if(i > 0){queryBuilder.append(", ");}
            queryBuilder.append(entry.getKey());
            i++;
        }
        queryBuilder.append(") VALUES (");

        i = 0;
        for(Map.Entry<String, Tuple<Object, Integer>> entry : dataSet.entrySet()){
            if(i > 0){queryBuilder.append(", ");}
            queryBuilder.append(this.getValue(entry.getKey(), dataSet));
            i++;
        }
        queryBuilder.append(")");

        String query = queryBuilder.toString();
        return query;
    }

}
