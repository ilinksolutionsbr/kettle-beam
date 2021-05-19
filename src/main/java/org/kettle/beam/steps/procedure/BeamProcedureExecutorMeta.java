package org.kettle.beam.steps.procedure;

import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Classe respnsável por gerencier os meta dados spoon para o componente
 * Procedure Executor.
 *
 */
@Step(
        id = BeamConst.STRING_BEAM_PROCEDURE_EXECUTOR_PLUGIN_ID,
        name = "Beam Procedure Executor",
        description = "Procedure Executor",
        image = "beam-database-connector.svg",
        categoryDescription = "Big Data"
)
public class BeamProcedureExecutorMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String DATABASE_TYPE = "databaseType";
    public static final String SERVER = "server";
    public static final String PORT = "port";
    public static final String DATABASE_NAME = "databaseName";
    public static final String CONNECTION_STRING = "connectionString";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String QUERY = "query";
    public static final String CONNECTION_STRING_VIEW = "connectionStringView";

    private String databaseType;
    private String server;
    private String port;
    private String databaseName;
    private String connectionString;
    private String username;
    private String password;
    private String query;
    private String connectionStringView;

    public String getDatabaseType() {
        return databaseType;
    }
    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getServer() {
        return server;
    }
    public void setServer(String server) {
        this.server = server;
    }

    public String getPort() {
        return port;
    }
    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getConnectionString() {
        return connectionString;
    }
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }

    public String getQuery() {
        return query;
    }
    public void setQuery(String query) {
        this.query = query;
    }

    public String getConnectionStringView() {
        return connectionStringView;
    }
    public void setConnectionStringView(String connectionStringView) {
        this.connectionStringView = connectionStringView;
    }

    public BeamProcedureExecutorMeta() {
        super();
    }

    @Override
    public void setDefault() {
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        StepInterface step = null;
        if (BeamConst.STRING_BEAM_PROCEDURE_EXECUTOR_PLUGIN_ID.equalsIgnoreCase(stepMeta.getStepID())) {
            step = new BeamProcedureExecutor(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        }
        return step;
    }

    @Override
    public StepDataInterface getStepData() {
        return new BeamProcedureExecutorData();
    }

    @Override
    public String getXML() throws KettleException {
        StringBuffer xml = new StringBuffer();

        xml.append( XMLHandler.addTagValue( DATABASE_TYPE, this.getDatabaseType() ) );
        xml.append( XMLHandler.addTagValue( SERVER, this.getServer() ) );
        xml.append( XMLHandler.addTagValue( PORT, this.getPort() ) );
        xml.append( XMLHandler.addTagValue( DATABASE_NAME, this.getDatabaseName() ) );
        xml.append( XMLHandler.addTagValue( CONNECTION_STRING, this.getConnectionString() ) );
        xml.append( XMLHandler.addTagValue( USERNAME, this.getUsername() ) );
        xml.append( XMLHandler.addTagValue( PASSWORD, this.getPassword() ) );
        xml.append( XMLHandler.addTagValue( QUERY, this.getQuery() ) );
        xml.append( XMLHandler.addTagValue( CONNECTION_STRING_VIEW, this.getConnectionStringView() ) );

        return xml.toString();
    }

    @Override
    public void loadXML(Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {

        this.setDatabaseType(XMLHandler.getTagValue( stepNode, DATABASE_TYPE));
        this.setServer(XMLHandler.getTagValue( stepNode, SERVER));
        this.setPort(XMLHandler.getTagValue( stepNode, PORT));
        this.setDatabaseName(XMLHandler.getTagValue( stepNode, DATABASE_NAME));
        this.setConnectionString(XMLHandler.getTagValue( stepNode, CONNECTION_STRING));
        this.setUsername(XMLHandler.getTagValue( stepNode, USERNAME ));
        this.setPassword(XMLHandler.getTagValue( stepNode, PASSWORD ));
        this.setQuery(XMLHandler.getTagValue( stepNode, QUERY ));
        this.setConnectionStringView(XMLHandler.getTagValue( stepNode, CONNECTION_STRING_VIEW ));
    }
}
