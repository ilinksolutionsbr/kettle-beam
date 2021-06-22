package org.kettle.beam.steps.formatter;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.steps.bq.BQField;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.math.BigDecimal;
import java.util.Calendar;

/**
 * Dialog de configuração do Step para converter string em JSON
 * @author Renato Dornelas Cardoso
 */
public class BeamJSONParserDialog extends BaseStepDialog implements StepDialogInterface {

    //region Attributes

    private static Class<?> PKG = BeamJSONParserDialog.class;
    private BeamJSONParserMeta input;

    private String stepName;
    private int middle;
    private int margin;

    private Combo cboJsonField;
    private TableView wFields;


    private RowMetaInterface rowMeta;

    //endregion

    //region Constructors

    /**
     * Construtor padrão
     *
     * @param parent
     * @param in
     * @param transMeta
     * @param sname
     */
    public BeamJSONParserDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (BeamJSONParserMeta) in;
    }

    //endregion

    //region Methods

    /**
     * Método para abrir a tela.
     *
     * @return
     */
    @Override
    public String open() {

        Shell parent = this.getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.input);

        this.changed = this.input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        FormData formData;

        this.shell.setLayout( formLayout );
        this.shell.setText( BaseMessages.getString( PKG, "BeamJSONParserDialog.DialogTitle" ) );

        this.middle = this.props.getMiddlePct();
        this.margin = Const.MARGIN;


        String[] fieldNames;
        try {
            fieldNames = this.transMeta.getPrevStepFields(this.stepMeta).getFieldNames();
        } catch( KettleException e ) {
            log.logError("Error getting fields from previous steps", e);
            fieldNames = new String[] {};
        }



        // Stepname line
        wlStepname = new Label( shell, SWT.RIGHT );
        wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
        props.setLook( wlStepname );
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment( 0, 0 );
        fdlStepname.top = new FormAttachment( 0, margin );
        fdlStepname.right = new FormAttachment( middle, -margin );
        wlStepname.setLayoutData( fdlStepname );
        wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        wStepname.setText( stepname );
        props.setLook( wStepname );
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment( middle, 0 );
        fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
        fdStepname.right = new FormAttachment( 100, 0 );
        wStepname.setLayoutData( fdStepname );
        Control lastControl = wStepname;


        Label lblJsonField = new Label( shell, SWT.RIGHT );
        lblJsonField.setText( BaseMessages.getString( PKG, "BeamJSONParserDialog.JsonField" ) );
        props.setLook(lblJsonField);
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblJsonField.setLayoutData(formData);
        this.cboJsonField = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( this.cboJsonField);
        this.cboJsonField.setItems( fieldNames);
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment( lblJsonField, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.cboJsonField.setLayoutData(formData);
        lastControl = this.cboJsonField;

        Label wlFields = new Label( shell, SWT.LEFT );
        wlFields.setText( BaseMessages.getString( PKG, "BeamJSONParserDialog.Fields" ) );
        props.setLook( wlFields );
        FormData fdlFields = new FormData();
        fdlFields.left = new FormAttachment( 0, 0 );
        fdlFields.top = new FormAttachment( lastControl, margin );
        fdlFields.right = new FormAttachment( middle, -margin );
        wlFields.setLayoutData( fdlFields );

        this.wOK = new Button( this.shell, SWT.PUSH );
        this.wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        this.wGet = new Button(shell, SWT.PUSH);
        this.wGet.setText(BaseMessages.getString( PKG, "System.Button.GetFields" ) );
        this.wCancel = new Button( this.shell, SWT.PUSH );
        this.wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
        this.setButtonPositions( new Button[] { this.wOK, this.wGet, this.wCancel }, this.margin, null );

        ColumnInfo[] columns = new ColumnInfo[] {
                new ColumnInfo( BaseMessages.getString( PKG, "BeamJSONParserDialog.Fields.Column.Name" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "BeamJSONParserDialog.Fields.Column.NewName" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "BeamJSONParserDialog.Fields.Column.KettleType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), false ),
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

        this.lsOK = e -> this.ok();
        this.lsCancel = e -> this.cancel();

        this.wOK.addListener( SWT.Selection, this.lsOK );
        this.wGet.addListener( SWT.Selection, e-> getFields() );
        this.wCancel.addListener( SWT.Selection, this.lsCancel );

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected( SelectionEvent e ) {
                BeamJSONParserDialog.this.ok();
            }
        };

        wStepname.addSelectionListener( this.lsDef );
        this.cboJsonField.addSelectionListener( this.lsDef );


        // Detect X or ALT-F4 or something that kills this window...
        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed( ShellEvent e ) {
                BeamJSONParserDialog.this.cancel();
            }
        });

        this.getData( );
        setSize();
        this.input.setChanged(this.changed);

        this.shell.open();
        while ( !this.shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }
        return stepname;

    }

    public void getData( ) {
        this.wStepname.setText( stepname );
        this.cboJsonField.setText(Const.NVL(this.input.getJsonField(), ""));
        for (int i=0;i<input.getFields().size();i++) {
            JSONField field = input.getFields().get( i );
            TableItem item = wFields.table.getItem( i );
            item.setText( 1, Const.NVL(field.getName(), "") );
            item.setText( 2, Const.NVL(field.getNewName(), "") );
            item.setText( 3, Const.NVL(field.getKettleType(), "") );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
        this.wStepname.selectAll();
        this.wStepname.setFocus();
    }

    private void cancel() {
        this.stepname = null;
        this.input.setChanged(this.changed);
        dispose();
    }

    private void ok() {
        try {
            if (Utils.isEmpty(wStepname.getText())) {return;}
            if (Strings.isNullOrEmpty(cboJsonField.getText()) ) {throw new Exception("Campo do Json nao informado.");}
            input.setJsonField(cboJsonField.getText());
            input.getFields().clear();
            for (int i=0;i<wFields.nrNonEmpty();i++) {
                TableItem item = wFields.getNonEmpty( i );
                String name = item.getText(1);
                String newName = item.getText(2);
                String kettleType = item.getText(3);
                input.getFields().add(new JSONField( name, newName, kettleType ));
            }
            input.setChanged();
            dispose();

        }catch (Exception ex){
            SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

        }
    }

    private void getFields() {
        BeamJSONParserModelDialog dialog = new BeamJSONParserModelDialog(this);
        dialog.open();
    }

    public void setFields(java.util.List<JSONField> fields) {
        wFields.clearAll();
        for (int i=0;i<fields.size();i++) {
            JSONField field = fields.get( i );
            TableItem item = new TableItem(wFields.table, SWT.NONE);
            item.setText( 1, Const.NVL(field.getName(), "") );
            item.setText( 2, Const.NVL(field.getNewName(), "") );
            item.setText( 3, Const.NVL(field.getKettleType(), "") );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
    }


    //endregion

}
