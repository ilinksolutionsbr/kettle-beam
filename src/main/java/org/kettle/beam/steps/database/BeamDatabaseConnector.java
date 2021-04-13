package org.kettle.beam.steps.database;

import com.google.cloud.Tuple;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.sql.*;
import java.util.*;

/**
 * Classe responsável por execução do step
 * Database Connector.
 *
 * @author Renato Dornelas Cardoso <renato@romaconsulting.com.br>
 */
public class BeamDatabaseConnector extends BaseStep implements StepInterface {


    //region Constructors

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
    public BeamDatabaseConnector(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }

    //endregion

    //region Methods

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

        boolean result;

        BeamDatabaseConnectorMeta meta = (BeamDatabaseConnectorMeta)smi;
        BeamDatabaseConnectorData data = (BeamDatabaseConnectorData)sdi;
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            String database = this.getParentVariableSpace().environmentSubstitute(meta.getDatabase());
            String connectionString = this.getParentVariableSpace().environmentSubstitute(meta.getConnectionString());
            String username = this.getParentVariableSpace().environmentSubstitute(meta.getUsername());
            String password = this.getParentVariableSpace().environmentSubstitute(meta.getPassword());
            String queryType = this.getParentVariableSpace().environmentSubstitute(meta.getQueryType());
            String sql = meta.getQuery();
            String driver = BeamDatabaseConnectorHelper.getInstance().getDriver(database);

            List<String> parameters = new ArrayList<>();
            sql = BeamDatabaseConnectorHelper.getInstance().prepareSQL(sql, parameters);

            Class.forName(driver);
            connection = DriverManager.getConnection(connectionString, username, password);
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sql);

            if(parameters.size() > 0) {
                Object[] row = this.getRow();
                Map<String, Tuple<Object, Integer>> dataSet = this.getDateSet(row);
                this.setParameters(preparedStatement, parameters, dataSet);
            }

            if(BeamDatabaseConnectorHelper.QUERY_TYPE_SELECT.equalsIgnoreCase(queryType)){
                this.executeQuery(connection, preparedStatement, meta);
                this.setOutputDone();
                result = false;

            }else{
                this.executeNonQuery(connection, preparedStatement);
                result = true;
            }


        }catch (Exception ex){
            if(connection !=null){
                try{connection.rollback();}catch (Exception e){}
            }
            this.log.logError(BeamDatabaseConnector.class.getName() + " -> " + ex.getMessage(), ex);
            this.setOutputDone();
            result = false;

        }finally {
            if(preparedStatement != null){
                try{preparedStatement.close();}catch (Exception e){}
            }
            if(connection !=null){
                try{connection.close();}catch (Exception e){}
            }
        }

        return result;

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

    private void setParameters(PreparedStatement preparedStatement, List<String> parameters, Map<String, Tuple<Object, Integer>> dataSet) throws Exception{
        Object value;
        Tuple<Object, Integer> tuple;
        int i = 0;
        for(String parameter : parameters){
            i++;
            if(dataSet.containsKey(parameter)){
                tuple = dataSet.get(parameter);
                if(tuple.x() != null){
                    preparedStatement.setObject(i, tuple.x());
                }else{
                    preparedStatement.setNull(i, tuple.y());
                }

            }else{
                value = this.getParentVariableSpace().environmentSubstitute("${" + parameter + "}");
                if(value != null && value.toString().equals(parameter)){
                    value = null;
                }
                if(value != null){
                    try{
                        value = Integer.parseInt(value.toString());
                    }catch (Exception ex){}
                    preparedStatement.setObject(i, value);
                }else{
                    preparedStatement.setNull(i, Types.VARCHAR);
                }
            }
        }
    }

    private void executeNonQuery(Connection connection, PreparedStatement preparedStatement) throws Exception{
        int count = preparedStatement.executeUpdate();
        RowMeta outputRowMeta = new RowMeta();
        outputRowMeta.addValueMeta(new ValueMetaInteger("rows_count"));
        Object[] row = new Object[]{count};
        this.putRow(outputRowMeta, row);
        connection.commit();
        if (isRowLevel()) {
            logRowlevel("Beam Database Connector", outputRowMeta.getString(row));
        }
    }

    private void executeQuery(Connection connection, PreparedStatement preparedStatement, BeamDatabaseConnectorMeta meta) throws Exception{
        ResultSet resultSet = preparedStatement.executeQuery();

        RowMeta outputRowMeta = new RowMeta();
        ValueMetaInterface valueMeta;

        Object[] row;
        List<String> columns = new ArrayList<>();
        List<Integer> columnsOriginalType = new ArrayList<>();

        String columnName;
        int columnType;

        if(meta.getFields().size() > 0) {
            for(FieldInfo fieldInfo : meta.getFields()){
                for(int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    columnName = resultSet.getMetaData().getColumnName(i);
                    columnType = resultSet.getMetaData().getColumnType(i);
                    if(fieldInfo.getColumn().equalsIgnoreCase(columnName)){
                        valueMeta = this.createValueMeta(fieldInfo);
                        outputRowMeta.addValueMeta(valueMeta);
                        columns.add(columnName);
                        columnsOriginalType.add(columnType);
                        break;
                    }
                }
            }
        }else{
            for(int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                columnName = resultSet.getMetaData().getColumnName(i);
                columnType = resultSet.getMetaData().getColumnType(i);
                valueMeta = BeamConst.createValueMeta(columnName, columnType);
                outputRowMeta.addValueMeta(valueMeta);
                columns.add(columnName);
                columnsOriginalType.add(columnType);
            }
        }

        Object value;
        int j = -1;
        while(resultSet.next()){
            j++;
            row = new Object[columns.size()];
            for(int i = 0; i < columns.size(); i++) {
                value = resultSet.getObject(columns.get(i));
                //Inicia a conversão de tipos
                if(value == null){
                    row[i] = value;
                } else {
                    row[i] = castToSupportedType(value, columnsOriginalType.get(i));
                }
            }
            this.putRow(outputRowMeta, row);
            if (isRowLevel()) {
                logRowlevel("Beam Database Connector", outputRowMeta.getString(row));
            }
        }
    }

    private Object castToSupportedType(Object value, int type){

        switch(type){

            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                value = Long.valueOf((Integer) value);
                break;
        }

        return value;
    }

    private ValueMetaInterface createValueMeta(FieldInfo fieldInfo){
        if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_BIG_NUMBER)){
            return new ValueMetaBigNumber(fieldInfo.getName());

        } else if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_BINARY)) {
            return new ValueMetaBinary(fieldInfo.getName());

        } else if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_BOOLEAN)) {
            return new ValueMetaBoolean(fieldInfo.getName());

        } else if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_DATE)) {
            return new ValueMetaDate(fieldInfo.getName());

        } else if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_INTEGER)) {
            return new ValueMetaInteger(fieldInfo.getName());

        } else if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_INTERNET_ADDRESS)) {
            return new ValueMetaInternetAddress(fieldInfo.getName());

        } else if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_NUMBER)) {
            return new ValueMetaNumber(fieldInfo.getName());

        } else if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_STRING)) {
            return new ValueMetaString(fieldInfo.getName());

        } else if(fieldInfo.getType().equalsIgnoreCase(BeamDatabaseConnectorHelper.BEAM_DATATYPE_TIMESTAMP)) {
            return new ValueMetaTimestamp(fieldInfo.getName());

        } else{
            return new ValueMetaString(fieldInfo.getName());
        }
    }

    //endregion
}
