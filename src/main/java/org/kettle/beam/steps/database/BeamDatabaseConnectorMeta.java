package org.kettle.beam.steps.database;

import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Classe respnsável por gerencier os meta dados spoon para o componente
 * Database Connector.
 *
 * @author Renato Dornelas Cardoso <renato@romaconsulting.com.br>
 */
@Step(
        id = BeamConst.STRING_BEAM_DATABASE_PLUGIN_ID,
        name = "Beam Database Connector",
        description = "Database Connector",
        image = "beam-database-connector.svg",
        categoryDescription = "Big Data"
)
public class BeamDatabaseConnectorMeta extends BaseStepMeta implements StepMetaInterface {

    //region Attributes

    public static final String DATABASE_TYPE = "databaseType";
    public static final String SERVER = "server";
    public static final String PORT = "port";
    public static final String DATABASE_NAME = "databaseName";
    public static final String CONNECTION_STRING = "connectionString";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String QUERY_TYPE = "queryType";
    public static final String QUERY = "query";

    private String databaseType;
    private String server;
    private String port;
    private String databaseName;
    private String connectionString;
    private String username;
    private String password;
    private String queryType;
    private String query;
    private List<FieldInfo> fields;
    private StepDataInterface stepData;

    //endregion

    //region Getters Setters

    public List<FieldInfo> getFields(){
        if(this.fields == null){this.fields = new ArrayList<>();}
        return this.fields;
    }

    public String getDatabaseType(){return this.databaseType;}
    public void setDatabaseType(String value){this.databaseType = value;}

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

    public String getConnectionString(){return this.connectionString;}
    public void setConnectionString(String value){this.connectionString = value;}

    public String getUsername(){return this.username;}
    public void setUsername(String value){this.username = value;}

    public String getPassword(){return this.password;}
    public void setPassword(String value){this.password = value;}

    public String getQueryType(){return this.queryType;}
    public void setQueryType(String value){this.queryType = value;}

    public String getQuery(){return this.query;}
    public void setQuery(String value){this.query = value;}

    //endregion

    //region Constructors

    public BeamDatabaseConnectorMeta(){super();}

    //endregion

    //region Methods

    @Override
    public void setDefault() {
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        StepInterface step = null;
        if (BeamConst.STRING_BEAM_DATABASE_PLUGIN_ID.equalsIgnoreCase(stepMeta.getStepID())) {
            step = new BeamDatabaseConnector(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        }
        return step;
    }

    @Override
    public StepDataInterface getStepData() {
        return new BeamDatabaseConnectorData();
    }

    @Override
    public String getDialogClassName() {
        return BeamDatabaseConnectorDialog.class.getName();
    }

    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
        try {
            for (FieldInfo field : this.getFields()) {
                int type = ValueMetaFactory.getIdForValueMeta(field.getType());
                ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( field.getName(), type, -1, -1 );
                valueMeta.setOrigin( name );
                inputRowMeta.addValueMeta( valueMeta );
            }
        } catch ( Exception e ) {
            throw new KettleStepException( "Error getting Beam Database Connector", e );
        }
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
        xml.append( XMLHandler.addTagValue( QUERY_TYPE, this.getQueryType() ) );
        xml.append( XMLHandler.addTagValue( QUERY, this.getQuery() ) );

        xml.append( XMLHandler.openTag( "fields" ) );
        for ( FieldInfo field : this.getFields() ) {
            xml.append( XMLHandler.openTag( "field" ) );
            xml.append( XMLHandler.addTagValue( "column", field.getColumn() ) );
            xml.append( XMLHandler.addTagValue( "variable", field.getVariable() ) );
            xml.append( XMLHandler.addTagValue( "type", field.getType() ) );
            xml.append( XMLHandler.closeTag( "field" ) );
        }
        xml.append( XMLHandler.closeTag( "fields" ) );

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
        this.setQueryType(XMLHandler.getTagValue( stepNode, QUERY_TYPE ));
        this.setQuery(XMLHandler.getTagValue( stepNode, QUERY ));

        Node fieldsNode = XMLHandler.getSubNode( stepNode, "fields" );
        List<Node> fieldNodes = XMLHandler.getNodes( fieldsNode, "field" );
        String from;
        String to;
        String type;
        for ( Node fieldNode : fieldNodes ) {
            from = XMLHandler.getTagValue( fieldNode, "column" );
            to = XMLHandler.getTagValue( fieldNode, "variable" );
            type = XMLHandler.getTagValue( fieldNode, "type" );
            this.getFields().add( new FieldInfo(from, to, type ) );
        }
    }

    //endregion

}
