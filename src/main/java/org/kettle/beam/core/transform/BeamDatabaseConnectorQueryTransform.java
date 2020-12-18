package org.kettle.beam.core.transform;

import com.google.cloud.Tuple;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
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

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeamDatabaseConnectorQueryTransform extends PTransform<PBegin, PCollection<KettleRow>> {

    //region Attributes

    private String database;
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
    private static final Counter numRead = Metrics.counter( "main", "BeamDatabaseConnectorQuery" );

    //endregion

    //region Constructors

    public BeamDatabaseConnectorQueryTransform() {}

    public BeamDatabaseConnectorQueryTransform(String stepname
            , String database
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

        this.database = database;
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
    public PCollection<KettleRow> expand(PBegin input) {
        try {
            BeamKettle.init(stepPluginClasses, xpPluginClasses);

            PCollection<KettleRow> output = input
                    .apply(JdbcIO.<KettleRow>read()
                    .withDataSourceConfiguration(
                            JdbcIO.DataSourceConfiguration.create(this.driver, this.connectionString)
                                    .withUsername(this.username)
                                    .withPassword(this.password))
                    .withQuery(this.query)
                    .withStatementPreparator(new JdbcIO.StatementPreparator() {
                        @Override
                        public void setParameters(PreparedStatement preparedStatement) throws Exception {
                            BeamDatabaseConnectorQueryTransform.this.setPreparedStatement(preparedStatement);
                        }
                    })
                    .withRowMapper(new JdbcIO.RowMapper<KettleRow>() {
                        @Override
                        public KettleRow mapRow(ResultSet resultSet) throws Exception {
                            numRead.inc();
                            return BeamDatabaseConnectorQueryTransform.this.parse(resultSet);
                        }
                    })
                    .withCoder(SerializableCoder.of(KettleRow.class))
            );

            return output;

        } catch ( Exception e ) {
            numErrors.inc();
            LOG.error( "Error in Beam Database Connector Query transform", e );
            throw new RuntimeException( "Error in Beam Database Connector Query transform", e );
        }

    }

    private void setPreparedStatement(PreparedStatement preparedStatement) throws Exception {
        Object value;
        int i = 0;
        for(String parameter : this.parameters){
            i++;
            if(this.configuration.containsKey(parameter)){
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

    }

    public KettleRow parse(ResultSet resultSet) throws SQLException {
        Object[] row;
        List<String> columns = new ArrayList<>();

        String columnName;

        if(this.fields.size() > 0) {
            for(FieldInfo fieldInfo : this.fields){
                for(int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    columnName = resultSet.getMetaData().getColumnName(i);
                    if(fieldInfo.getColumn().equalsIgnoreCase(columnName)){
                        columns.add(columnName);
                        break;
                    }
                }
            }
        }else{
            for(int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                columnName = resultSet.getMetaData().getColumnName(i);
                columns.add(columnName);
            }
        }

        Object value;
        row = new Object[columns.size()];
        for(int i = 0; i < columns.size(); i++) {
            value = resultSet.getObject(columns.get(i));
            row[i] = value;
        }

        KettleRow kettleRow = new KettleRow();
        kettleRow.setRow(row);

        return kettleRow;
    }

    //endregion

}
