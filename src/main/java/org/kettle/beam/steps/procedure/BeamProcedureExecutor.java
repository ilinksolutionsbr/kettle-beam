package org.kettle.beam.steps.procedure;

import com.google.cloud.Tuple;
import org.kettle.beam.steps.database.BeamDatabaseConnectorHelper;
import org.kettle.beam.util.DatabaseUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Classe responsável por execução do step
 * Procedure Executor.
 */
public class BeamProcedureExecutor extends BaseStep implements StepInterface {

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
    public BeamProcedureExecutor(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

        boolean result;

        BeamProcedureExecutorMeta meta = (BeamProcedureExecutorMeta)smi;
        BeamProcedureExecutorData data = (BeamProcedureExecutorData)sdi;
        Connection connection = null;
        CallableStatement callableStatement = null;

        try {
            String databaseType = this.getParentVariableSpace().environmentSubstitute(meta.getDatabaseType());
            String connectionString = DatabaseUtil.getConnectionUrl(databaseType, meta.getServer(), meta.getPort(), meta.getDatabaseName(), meta.getConnectionString());
            String connectionUrl = this.getParentVariableSpace().environmentSubstitute(connectionString);
            String username = this.getParentVariableSpace().environmentSubstitute(meta.getUsername());
            String password = this.getParentVariableSpace().environmentSubstitute(meta.getPassword());
            String sql = meta.getQuery();
            String driver = BeamDatabaseConnectorHelper.getInstance().getDriver(databaseType);

            List<String> parameters = new ArrayList<>();
            sql = DatabaseUtil.prepareSQL(sql, parameters);

            Class.forName(driver);
            connection = DriverManager.getConnection(connectionUrl, username, password);
            callableStatement = connection.prepareCall(sql);

            if(parameters.size() > 0) {
                Object[] row = this.getRow();
                Map<String, Tuple<Object, Integer>> dataSet = this.getDateSet(row);
                this.setParameters(callableStatement, parameters, dataSet);
            }

            this.executeProcedure(connection, callableStatement);
            this.setOutputDone();
            result = false;

        }catch (Exception ex){
            if(connection !=null){
                try{connection.rollback();}catch (Exception e){}
            }
            this.log.logError(BeamProcedureExecutor.class.getName() + " -> " + ex.getMessage(), ex);
            this.setOutputDone();
            result = false;

        }finally {
            if(callableStatement != null){
                try{callableStatement.close();}catch (Exception e){}
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

    private void setParameters(CallableStatement callableStatement, List<String> parameters, Map<String, Tuple<Object, Integer>> dataSet) throws Exception{
        Object value;
        Tuple<Object, Integer> tuple;
        int i = 0;
        for(String parameter : parameters){
            i++;
            if(dataSet.containsKey(parameter)){
                tuple = dataSet.get(parameter);
                if(tuple.x() != null){
                    callableStatement.setObject(i, tuple.x());
                }else{
                    callableStatement.setNull(i, tuple.y());
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
                    callableStatement.setObject(i, value);
                }else{
                    callableStatement.setNull(i, Types.VARCHAR);
                }
            }
        }
    }

    private void executeProcedure(Connection connection, CallableStatement callableStatement) throws Exception{
        int count = callableStatement.executeUpdate();
        RowMeta outputRowMeta = new RowMeta();
        outputRowMeta.addValueMeta(new ValueMetaInteger("rows_count"));
        Object[] row = new Object[]{count};
        this.putRow(outputRowMeta, row);
        if (isRowLevel()) {
            logRowlevel("Beam Procedure Executor", outputRowMeta.getString(row));
        }
    }
}
