package org.kettle.beam.steps.firestore;

import java.awt.Desktop;
import java.net.URI;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.cloud.datastore.*;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.kettle.beam.core.KettleErrorDialog;
import org.kettle.beam.core.fn.FirestoreEntityToKettleRowFn;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Classe responsável por renderizar a tela de Dialogo de configuração do
 * componente.
 *
 * @author Thiago Teodoro Rodrigues <thiago.rodrigues@callink.com.br>
 */
public class BeamFirestoreInputDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = BeamFirestoreInput.class; // for i18n purposes, needed by Translator2!!
    private final BeamFirestoreInputMeta input;

    int middle;
    int margin;

    private TextVar wKind;
    private TableView wFields;

    /**
     * Construtor padrão
     *
     * @param parent
     * @param in
     * @param transMeta
     * @param sname
     */
    public BeamFirestoreInputDialog(Shell parent, Object in, TransMeta transMeta, String sname) {

        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (BeamFirestoreInputMeta) in;
    }

    /**
     * Método para abrir a tela.
     *
     * @return
     */
    public String open() {

        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        props.setLook(shell);
        setShellImage(shell, input);

        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "BeamInputFirestore.DialogTitle"));

        middle = props.getMiddlePct();
        margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.top = new FormAttachment(0, margin);
        fdlStepname.right = new FormAttachment(middle, -margin);
        wlStepname.setLayoutData(fdlStepname);
        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(wlStepname, 0, SWT.CENTER);
        fdStepname.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fdStepname);
        Control lastControl = wStepname;

        Label wlKind = new Label(shell, SWT.RIGHT);
        wlKind.setText(BaseMessages.getString(PKG, "BeamInputFirestore.Kind"));
        props.setLook(wlKind);
        FormData fdlKind = new FormData();
        fdlKind.left = new FormAttachment(0, 0);
        fdlKind.top = new FormAttachment(lastControl, margin);
        fdlKind.right = new FormAttachment(middle, -margin);
        wlKind.setLayoutData(fdlKind);
        wKind = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wKind);
        FormData fdEntity = new FormData();
        fdEntity.left = new FormAttachment(middle, 0);
        fdEntity.top = new FormAttachment(wlKind, 0, SWT.CENTER);
        fdEntity.right = new FormAttachment(100, 0);
        wKind.setLayoutData(fdEntity);
        lastControl = wKind;

        Label wlFields = new Label(shell, SWT.LEFT);
        wlFields.setText( BaseMessages.getString(PKG, "BeamInputFirestore.Fields" ));
        props.setLook( wlFields );
        FormData fdlFields = new FormData();
        fdlFields.left = new FormAttachment( 0, 0 );
        fdlFields.top = new FormAttachment( lastControl, margin );
        fdlFields.right = new FormAttachment( middle, -margin );
        wlFields.setLayoutData( fdlFields );

        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wGet = new Button(shell, SWT.PUSH);
        wGet.setText(BaseMessages.getString( PKG, "System.Button.GetFields" ));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{wOK, wGet, wCancel}, margin, null);

        ColumnInfo[] columns = new ColumnInfo[] {
                new ColumnInfo( BaseMessages.getString( PKG, "BeamInputFirestore.Fields.Column.Name" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "BeamInputFirestore.Fields.Column.NewName" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "BeamInputFirestore.Fields.Column.KettleType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), false ),
        };
        wFields = new TableView( Variables.getADefaultVariableSpace(), shell, SWT.NONE, columns, input.getFields().size(), null, props);
        props.setLook( wFields );
        FormData fdFields = new FormData();
        fdFields.left = new FormAttachment( 0, 0 );
        fdFields.top = new FormAttachment( wlFields, margin );
        fdFields.right = new FormAttachment( 100, 0 );
        fdFields.bottom = new FormAttachment( wOK, -2*margin);
        wFields.setLayoutData( fdFields );
        lastControl = wFields;

        // Add listeners
        wOK.addListener(SWT.Selection, e -> ok());
        wGet.addListener( SWT.Selection, e-> getFields() );
        wCancel.addListener(SWT.Selection, e -> cancel());

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        wStepname.addSelectionListener(lsDef);
        wKind.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addListener(SWT.Close, e -> cancel());

        getData();
        setSize();
        input.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return stepname;
    }

    /**
     * Método responsável por criar e o botão de ajuda que quando clicado
     * abre uma pagína de ajuda para o componente.
     * 
     * @param shell
     * @param stepMeta
     * @param plugin
     * @return 
     */
    @Override
    protected Button createHelpButton(Shell shell, StepMeta stepMeta, PluginInterface plugin) {

        Button helpButton = new Button(shell, SWT.PUSH);

        helpButton.setText(BaseMessages.getString( PKG, "BeamInputFirestore.HelpButton" ));

        helpButton.addListener(SWT.Selection, e ->
            openUrlHelp(BaseMessages.getString( PKG, "BeamInputFirestore.HelpLink" ))
        );
        return helpButton;
    }

    public void getFields() {
        try {
            RowMetaInterface rowMeta = new RowMeta();

            if(!wKind.getText().isEmpty()) {
                Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

                Query<Entity> query = Query.newEntityQueryBuilder()
                        .setKind(wKind.getText())
                        .setLimit(1)
                        .build();

                QueryResults<Entity> results = datastore.run(query);

                if(results.hasNext()) {
                    while (results.hasNext()) {
                        Entity currentEntity = results.next();
                        Map<String, Value<?>> properties = currentEntity.getProperties();
                        wFields.clearAll();

                        // Using Avro Type for conversion
                        properties.forEach((name, value) -> {
                            try {
                                String type = value.getType().name();

                                int kettleType = FirestoreEntityToKettleRowFn.AvroType.valueOf( type ).getKettleType();
                                rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( name, kettleType ) );
                            } catch (KettlePluginException ke) {
                                throw new RuntimeException(ke);
                            }
                        });
                    }
                } else {
                    SimpleMessageDialog.openWarning(this.shell, "Aviso", "A kind informada não existe ou não possui nenhum registro para obter os campos");
                }
            } else {
                SimpleMessageDialog.openWarning(this.shell, "Aviso", "O nome da kind deve ser informado para obter os campos");
            }

            BaseStepDialog.getFieldsFromPrevious( rowMeta, wFields, 1, new int[] { 1 }, new int[] { 3 }, -1, -1, true, null );
        } catch ( Exception e ) {
            new KettleErrorDialog( shell, "Error", "Error getting Firestore fields", e );
        }
    }

    /**
     * Método responsável por abrir uma no Browser.
     */
    private void openUrlHelp(String url) {
        
        if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
            
            try {
                
                Desktop.getDesktop().browse(new URI(url));
            } catch (Exception ex) {;
            
                this.log.logDetailed(BeamConst.STRING_BEAM_FIRESTORE_INPUT_PLUGIN_ID + "-> Ocorreu um erro inesperado em openUrlHelp(). Exception : ", ex);
                Logger.getLogger(BeamFirestoreInputDialog.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Populate the widgets.
     */
    public void getData() {
        wStepname.setText(stepname);
        wKind.setText(Const.NVL(input.getKind(), ""));

        for (int i=0;i<input.getFields().size();i++) {
            FirestoreField field = input.getFields().get( i );
            TableItem item = wFields.table.getItem( i );
            item.setText( 1, Const.NVL(field.getName(), "") );
            item.setText( 2, Const.NVL(field.getNewName(), "") );
            item.setText( 3, Const.NVL(field.getKettleType(), "") );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );

        wStepname.selectAll();
        wStepname.setFocus();
    }

    /**
     * Botão de cancelamento.
     */
    private void cancel() {
        stepname = null;
        input.setChanged(changed);
        dispose();
    }

    /**
     * Botão de aceite.
     */
    private void ok() {
        try {
            if (Utils.isEmpty(wStepname.getText())) {return;}
            if (Strings.isNullOrEmpty(wKind.getText()) ) {throw new Exception("Entidade de leitura nao informado.");}
            getInfo(input);
            dispose();

        }catch (Exception ex){
            SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

        }
    }

    /**
     * Obtendo informações.
     *
     * @param in
     */
    private void getInfo(BeamFirestoreInputMeta in) {

        stepname = wStepname.getText(); // return value

        in.setKind(wKind.getText());
        in.getFields().clear();
        for (int i=0;i<wFields.nrNonEmpty();i++) {
            TableItem item = wFields.getNonEmpty( i );
            String name = item.getText(1);
            String newName = item.getText(2);
            String kettleType = item.getText(3);
            in.getFields().add(new FirestoreField( name, newName, kettleType ));
        }

        input.setChanged();
    }

}
