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
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Dialog de configuração do Step para converter string em JSON
 * @author Renato Dornelas Cardoso
 */
public class BeamJSONParserDialog extends BaseStepDialog implements StepDialogInterface {

    //region Attributes

    private static Class<?> PACKAGE = BeamJSONParserDialog.class;
    private BeamJSONParserMeta input;

    private String stepName;
    private int middle;
    private int margin;

    private Combo cboJsonField;

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
        this.shell.setText( BaseMessages.getString( PACKAGE, "BeamJSONParserDialog.DialogTitle" ) );

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
        wlStepname.setText( BaseMessages.getString( PACKAGE, "System.Label.StepName" ) );
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
        lblJsonField.setText( BaseMessages.getString( PACKAGE, "BeamJSONParserDialog.JsonField" ) );
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



        this.wOK = new Button( this.shell, SWT.PUSH );
        this.wOK.setText( BaseMessages.getString( PACKAGE, "System.Button.OK" ) );
        this.wCancel = new Button( this.shell, SWT.PUSH );
        this.wCancel.setText( BaseMessages.getString( PACKAGE, "System.Button.Cancel" ) );
        this.setButtonPositions( new Button[] { this.wOK, this.wCancel }, this.margin, null );



        this.lsOK = e -> this.ok();
        this.lsCancel = e -> this.cancel();

        this.wOK.addListener( SWT.Selection, this.lsOK );
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
            input.setChanged();
            dispose();

        }catch (Exception ex){
            SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

        }
    }

    //endregion

}
