package org.kettle.beam.steps.firestore;

import java.util.ArrayList;
import java.util.List;

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
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Classe respnsável por gerencier os meta dados spoon para o componente
 * Firestore Input.
 * 
 * @author Thiago Teodoro Rodrigues <thiago.rodrigues@callink.com.br>
 */
@Step(
  id = BeamConst.STRING_BEAM_FIRESTORE_INPUT_PLUGIN_ID,
  name = "Beam Firestore Input",
  description = "Input data from GCP Firestore",
  image = "beam-gcp-firestore-input.svg",
  categoryDescription = "Big Data"
)
public class BeamFirestoreInputMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String KIND = "kind";

    private String kind;
    private String gqlQuery;

    private List<FirestoreField> fields;

    public BeamFirestoreInputMeta() {
        super();
        fields = new ArrayList<>();
    }

    @Override public void setDefault() {
    }

    @Override public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        return new BeamFirestoreInput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override public StepDataInterface getStepData() {
        return new BeamFirestoreInputData();
    }

    @Override public String getDialogClassName() {
        return BeamFirestoreInputDialog.class.getName();
    }

    @Override public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

        try {
            for ( FirestoreField field : fields ) {
                int type = ValueMetaFactory.getIdForValueMeta( field.getKettleType() );
                ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( field.getNewNameOrName(), type, -1, -1 );
                valueMeta.setOrigin( name );
                inputRowMeta.addValueMeta( valueMeta );
            }
        } catch ( Exception e ) {
            throw new KettleStepException( "Error getting Beam Firestore Input step output", e );
        }
    }

    @Override
    public String getXML() throws KettleException {
        
        StringBuffer xml = new StringBuffer();

        xml.append(XMLHandler.addTagValue(KIND, kind));
        xml.append( XMLHandler.openTag( "fields" ) );
        for ( FirestoreField field : fields ) {
            xml.append( XMLHandler.openTag( "field" ) );
            xml.append( XMLHandler.addTagValue( "name", field.getName() ) );
            xml.append( XMLHandler.addTagValue( "new_name", field.getNewName() ) );
            xml.append( XMLHandler.addTagValue( "type", field.getKettleType() ) );
            xml.append( XMLHandler.closeTag( "field" ) );
        }
        xml.append( XMLHandler.closeTag( "fields" ) );
        return xml.toString();
    }

    @Override
    public void loadXML(Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        
        kind = XMLHandler.getTagValue(stepNode, KIND);

        Node fieldsNode = XMLHandler.getSubNode( stepNode, "fields" );
        List<Node> fieldNodes = XMLHandler.getNodes( fieldsNode, "field" );
        fields = new ArrayList<>();
        for ( Node fieldNode : fieldNodes ) {
            String name = XMLHandler.getTagValue( fieldNode, "name" );
            String newName = XMLHandler.getTagValue( fieldNode, "new_name" );
            String kettleType = XMLHandler.getTagValue( fieldNode, "type" );
            fields.add( new FirestoreField( name, newName, kettleType ) );
        }
    }

    /**
     * Gets kind
     *
     * @return value of kind
     */
    public String getKind() {
        return kind;
    }

    /**
     * @param kind The kind to set
     */
    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getGqlQuery() {
        return gqlQuery;
    }

    public void setGqlQuery(String gqlQuery) {
        this.gqlQuery = gqlQuery;
    }

    public List<FirestoreField> getFields() {
        return fields;
    }

    public void setFields(List<FirestoreField> fields) {
        this.fields = fields;
    }
}
