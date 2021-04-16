package org.kettle.beam.core.transform;

import com.google.cloud.Tuple;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.steps.database.FieldInfo;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeamDatabaseConnectorUpdateTransform extends PTransform<PCollection<KettleRow>, PDone> {

    //region Attributes

    private String databaseType;
    private String driver;
    private String connectionString;
    private String username;
    private String password;
    private String queryType;
    private String query;
    private List<FieldInfo> fields;
    private List<String> parameters;
    private Map<String, String> configuration;

    private String stepname;
    private String rowMetaJson;

    private List<String> stepPluginClasses;
    private List<String> xpPluginClasses;

    private static final Logger LOG = LoggerFactory.getLogger( BeamDatabaseConnectorUpdateTransform.class );
    private static final Counter numErrors = Metrics.counter( "main", "BeamDatabaseConnectorError" );
    private static final Counter numUpdate = Metrics.counter( "main", "BeamDatabaseConnectorUpdate" );

    //endregion

    //region Constructors

    public BeamDatabaseConnectorUpdateTransform() {}

    public BeamDatabaseConnectorUpdateTransform(String stepname
            , String databaseType
            , String driver
            , String connectionString
            , String username
            , String password
            , String queryType
            , String query
            , List<FieldInfo> fields
            , List<String> parameters
            , Map<String, String> configuration
            , String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
        super(stepname);

        this.stepname = stepname;

        this.databaseType = databaseType;
        this.driver = driver;
        this.connectionString = connectionString;
        this.username = username;
        this.password = password;
        this.queryType = queryType;
        this.query = query;
        this.fields = fields;
        this.parameters = parameters;
        this.configuration = configuration;

        this.rowMetaJson = rowMetaJson;
        this.stepPluginClasses = stepPluginClasses;
        this.xpPluginClasses = xpPluginClasses;
    }

    //endregion

    //region Methods

    @Override
    public PDone expand(PCollection<KettleRow> input) {
        try {
            BeamKettle.init(stepPluginClasses, xpPluginClasses);
            return input.apply(JdbcIO.<KettleRow>write()
                    .withDataSourceConfiguration(
                            JdbcIO.DataSourceConfiguration.create(this.driver, this.connectionString)
                                    .withUsername(this.username)
                                    .withPassword(this.password))
                    .withStatement(this.query)
                    .withPreparedStatementSetter((element, preparedStatement) -> {
                        BeamDatabaseConnectorUpdateTransform.this.setPreparedStatement(element, preparedStatement);
                    }));

        } catch ( Exception e ) {
            numErrors.inc();
            LOG.error( "Error in Beam Database Connector Update transform", e );
            throw new RuntimeException( "Error in Beam Database Connector Update transform", e );
        }

    }
    private RowMetaInterface rowMeta;
    private void setPreparedStatement(KettleRow kettleRow, PreparedStatement preparedStatement) throws Exception {
        BeamKettle.init(stepPluginClasses, xpPluginClasses);
        if(rowMeta == null){
            rowMeta = JsonRowMeta.fromJson(this.rowMetaJson);
        }
        Map<String, Tuple<Object, Integer>> dataSet = this.getDateSet(rowMeta, kettleRow.getRow());
        Object value;
        Tuple<Object, Integer> tuple;
        int i = 0;
        for(String parameter : this.parameters){
            i++;
            if(dataSet.containsKey(parameter)){
                tuple = dataSet.get(parameter);
                if(tuple.x() != null){
                    preparedStatement.setObject(i, tuple.x());
                }else{
                    preparedStatement.setNull(i, tuple.y());
                }

            }else if(this.configuration.containsKey(parameter)){
                value = this.configuration.get(parameter);
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

            }else{
                preparedStatement.setNull(i, Types.VARCHAR);
            }
        }
        numUpdate.inc();
    }

    private Map<String, Tuple<Object, Integer>> getDateSet(RowMetaInterface rowMeta, Object[] row) throws KettleValueException {
        Map<String, Tuple<Object, Integer>> dataSet = new HashMap<>();
        if(row == null){return dataSet;}
        String field;
        Tuple<Object, Integer> tuple;
        int i = 0;
        for(ValueMetaInterface valueMetaInterface : rowMeta.getValueMetaList()){
            field = valueMetaInterface.getName().trim();
            tuple = null;
            switch (valueMetaInterface.getType()){
                case ValueMetaInterface.TYPE_STRING: tuple = Tuple.of(rowMeta.getString(row, i), Types.VARCHAR) ; break;
                case ValueMetaInterface.TYPE_INTEGER: tuple = Tuple.of(rowMeta.getInteger(row, i), Types.INTEGER); break;
                case ValueMetaInterface.TYPE_NUMBER: tuple = Tuple.of(rowMeta.getNumber(row, i), Types.NUMERIC); break;
                case ValueMetaInterface.TYPE_BIGNUMBER: tuple = Tuple.of(rowMeta.getBigNumber(row, i), Types.BIGINT); break;
                case ValueMetaInterface.TYPE_BOOLEAN: tuple = Tuple.of(rowMeta.getBoolean(row, i), Types.BOOLEAN); break;
                case ValueMetaInterface.TYPE_DATE: tuple = Tuple.of(rowMeta.getDate(row, i), Types.DATE); break;
                case ValueMetaInterface.TYPE_TIMESTAMP: tuple = Tuple.of(rowMeta.getDate(row, i), Types.TIMESTAMP); break;
                case ValueMetaInterface.TYPE_INET: tuple = Tuple.of(rowMeta.getString(row, i), Types.DATALINK); break;
                case ValueMetaInterface.TYPE_NONE: tuple = Tuple.of(rowMeta.getString(row, i), Types.BINARY); break;
                case ValueMetaInterface.TYPE_SERIALIZABLE: tuple = Tuple.of(rowMeta.getString(row, i), Types.BINARY); break;
            }
            if(tuple != null) {
                dataSet.put(field, tuple);
            }
            i++;
        }
        return dataSet;
    }

    //endregion

}
