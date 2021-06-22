package org.kettle.beam.steps.formatter;

import org.apache.commons.lang.StringUtils;
import org.kettle.beam.steps.bq.BQField;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaString;
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
 * Metadados do Step para converter string em JSON
 * @author Renato Dornelas Cardoso
 */
@Step(
        id = BeamConst.STRING_BEAM_JSON_PARSER_PLUGIN_ID,
        name = "Beam JSON Parser",
        description = "Conversor de String em Json",
        image = "beam-json-parser.svg",
        categoryDescription = "Big Data"
)
public class BeamJSONParserMeta extends BaseStepMeta implements StepMetaInterface {

    //region Attributes

    private static String JSON_FIELD = "jsonField";
    private static String FIELDS = "fields";
    private static String FIELD = "field";

    private String jsonField;
    private List<JSONField> fields;

    //endregion

    //region Constructors

    public BeamJSONParserMeta() {
        super();
    }

    //endregion

    //region Getters Setters

    public String getJsonField(){
        return jsonField;
    }
    public void setJsonField(String value){
        jsonField = value;
    }

    public List<JSONField> getFields(){
        if(fields == null){fields = new ArrayList<>();}
        return fields;
    }
    public void setFields(List<JSONField> value){
        fields = value;
    }

    //endregion

    //region Methods

    /**
     * Direcionamento de processamento.
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     * @return
     */
    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        StepInterface step = null;
        if (BeamConst.STRING_BEAM_JSON_PARSER_PLUGIN_ID.equalsIgnoreCase(stepMeta.getStepID())) {
            step = new BeamJSONParser(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        }
        return step;
    }

    /**
     * Setando valores padrões da Janela.
     */
    @Override
    public void setDefault() {
        this.jsonField = "";
        this.fields = new ArrayList<>();
    }

    /**
     * Etapa de apontamento da classe Data.
     * @return
     */
    @Override
    public StepDataInterface getStepData() {
        return new BeamJSONParserData();
    }

    /**
     * Apontamento para classe responsável pela Janela de Dialog do componente
     *
     * @return
     */
    @Override
    public String getDialogClassName() {
        return BeamJSONParserDialog.class.getName();
    }

    /**
     * Etapa de apontamento outpout no padrão beam iguinoramos essa etapa.
     *
     * @param inputRowMeta
     * @param name
     * @param info
     * @param nextStep
     * @param space
     * @param repository
     * @param metaStore
     * @throws KettleStepException
     */
    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
        try {
            if ( StringUtils.isEmpty(this.jsonField) ) {
                throw new KettleStepException( "Campo 'Json' não especificado." );
            }
            String jsonField = space.environmentSubstitute(this.jsonField);

            ValueMetaInterface valueMeta;

            valueMeta = new ValueMetaString(jsonField);
            valueMeta.setOrigin(name);
            inputRowMeta.addValueMeta(valueMeta);

            for ( JSONField field : fields ) {
                int type = ValueMetaFactory.getIdForValueMeta( field.getKettleType() );
                valueMeta = ValueMetaFactory.createValueMeta( field.getNewNameOrName(), type, -1, -1 );
                valueMeta.setOrigin( name );
                inputRowMeta.addValueMeta( valueMeta );
            }
        } catch ( Exception e ) {
            throw new KettleStepException( "Error getting Beam JSON Parser step output", e );
        }
    }

    /**
     * Configurações referentes a tela.
     *
     * @return
     * @throws KettleException
     */
    @Override
    public String getXML() throws KettleException {
        StringBuffer xml = new StringBuffer();
        xml.append(XMLHandler.addTagValue(JSON_FIELD, this.jsonField));

        xml.append( XMLHandler.openTag( FIELDS ) );
        for ( JSONField field : fields ) {
            xml.append( XMLHandler.openTag( FIELD ) );
            xml.append( XMLHandler.addTagValue( "name", field.getName() ) );
            xml.append( XMLHandler.addTagValue( "new_name", field.getNewName() ) );
            xml.append( XMLHandler.addTagValue( "type", field.getKettleType() ) );
            xml.append( XMLHandler.closeTag( FIELD ) );
        }
        xml.append( XMLHandler.closeTag( FIELDS ) );

        return xml.toString();
    }

    /**
     * Load dos dados da Tela.
     *
     * @param stepNode
     * @param databases
     * @param metaStore
     * @throws KettleXMLException
     */
    @Override
    public void loadXML(Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        this.jsonField = XMLHandler.getTagValue(stepNode, JSON_FIELD);
        Node fieldsNode = XMLHandler.getSubNode( stepNode, FIELDS );
        List<Node> fieldNodes = XMLHandler.getNodes( fieldsNode, FIELD );
        fields = new ArrayList<>();
        for ( Node fieldNode : fieldNodes ) {
            String name = XMLHandler.getTagValue( fieldNode, "name" );
            String newName = XMLHandler.getTagValue( fieldNode, "new_name" );
            String kettleType = XMLHandler.getTagValue( fieldNode, "type" );
            fields.add( new JSONField( name, newName, kettleType ) );
        }
    }

    //endregion

}
