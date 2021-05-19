package org.kettle.beam.steps.procedure;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.core.util.Web;
import org.kettle.beam.steps.database.BeamDatabaseConnectorHelper;
import org.kettle.beam.util.DatabaseUtil;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Classe responsável por criar a janela de configuração do step
 * Procedure Executor.
 */
public class BeamProcedureExecutorDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PACKAGE = BeamProcedureExecutorDialog.class;
    private BeamProcedureExecutorMeta metadata;

    private String stepName;
    private int middle;
    private int margin;

    private Combo cboDatabaseType;
    private TextVar txtServer;
    private TextVar txtPort;
    private TextVar txtDatabaseName;
    private TextVar txtConnectionString;
    private TextVar txtUsername;
    private TextVar txtPassword;
    private List lstVariables;
    private Label lblQuery;
    private TextVar txtQuery;
    private Text txtConnectionStringView;

    /**
     * Construtor padrão
     *
     * @param parent
     * @param metadata
     * @param transMeta
     * @param sname
     */
    public BeamProcedureExecutorDialog(Shell parent, Object metadata, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) metadata, transMeta, sname);
        this.metadata = (BeamProcedureExecutorMeta)metadata;
    }

    @Override
    public String open() {
        Shell parent = this.getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.metadata);

        this.changed = this.metadata.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        FormData formData;

        this.shell.setLayout( formLayout );
        this.shell.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.DialogTitle" ) );

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


        Label lblDatabaseType = new Label( shell, SWT.RIGHT );
        lblDatabaseType.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.DatabaseType" ) );
        props.setLook(lblDatabaseType);
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblDatabaseType.setLayoutData(formData);
        this.cboDatabaseType = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( this.cboDatabaseType);
        this.cboDatabaseType.setItems( BeamDatabaseConnectorHelper.getInstance().getDatabases());
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment( lblDatabaseType, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.cboDatabaseType.setLayoutData(formData);
        lastControl = this.cboDatabaseType;

        Label lblServer = new Label( shell, SWT.RIGHT );
        lblServer.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.Server" ) );
        props.setLook( lblServer );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblServer.setLayoutData(formData);
        this.txtServer = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook(this.txtServer);
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment(lblServer, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.txtServer.setLayoutData(formData);
        lastControl = this.txtServer;

        Label lblPort = new Label( shell, SWT.RIGHT );
        lblPort.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.Port" ) );
        props.setLook( lblPort );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblPort.setLayoutData(formData);
        this.txtPort = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook(this.txtPort);
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment(lblPort, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.txtPort.setLayoutData(formData);
        lastControl = this.txtPort;

        Label lblDbName = new Label( shell, SWT.RIGHT );
        lblDbName.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.DatabaseName" ) );
        props.setLook( lblDbName );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblDbName.setLayoutData(formData);
        this.txtDatabaseName = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook(this.txtDatabaseName);
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment(lblDbName, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.txtDatabaseName.setLayoutData(formData);
        lastControl = this.txtDatabaseName;

        Label lblConnectionString = new Label( shell, SWT.RIGHT );
        lblConnectionString.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.ConnectionString" ) );
        props.setLook( lblConnectionString );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblConnectionString.setLayoutData(formData);
        this.txtConnectionString = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook(this.txtConnectionString);
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment(lblConnectionString, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.txtConnectionString.setLayoutData(formData);
        lastControl = this.txtConnectionString;

        Label lblUsername = new Label( shell, SWT.RIGHT );
        lblUsername.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.Username" ) );
        props.setLook( lblUsername );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblUsername.setLayoutData(formData);
        this.txtUsername = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook(this.txtUsername);
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment(lblUsername, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.txtUsername.setLayoutData(formData);
        lastControl = this.txtUsername;


        Label lblPassword = new Label( shell, SWT.RIGHT );
        lblPassword.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.Password" ) );
        props.setLook( lblPassword );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblPassword.setLayoutData(formData);
        this.txtPassword = new PasswordTextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook(this.txtPassword);
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment(lblPassword, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.txtPassword.setLayoutData(formData);
        lastControl = this.txtPassword;


        Label lblConnectionStringView = new Label( shell, SWT.RIGHT );
        lblConnectionStringView.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.ConnectionStringView" ) );
        props.setLook( lblConnectionStringView );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblConnectionStringView.setLayoutData(formData);
        this.txtConnectionStringView = new Text( shell, SWT.LEFT );
        props.setLook(this.txtConnectionStringView);
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment( lblConnectionStringView, 0, SWT.CENTER  );
        formData.right = new FormAttachment( 100, 0 );
        this.txtConnectionStringView.setLayoutData(formData);
        this.txtConnectionStringView.setBackground(shell.getBackground());
        this.txtConnectionStringView.setEditable(false);
        lastControl = this.txtConnectionStringView;


        lblQuery = new Label( shell, SWT.LEFT );
        lblQuery.setText( BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.Query" ) );
        props.setLook( lblQuery );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( 100, 0 );
        lblQuery.setLayoutData( formData );

        this.lstVariables = new List( shell, SWT.SINGLE | SWT.BORDER );
        props.setLook( this.lstVariables );
        this.lstVariables.setItems(fieldNames);
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lblQuery, margin );
        formData.bottom = new FormAttachment( lblQuery, 250);
        formData.width = 120;
        this.lstVariables.setLayoutData(formData);
        this.lstVariables.addSelectionListener(new SelectionListener() {
            @Override
            public void widgetSelected(SelectionEvent selectionEvent) {
                if (BeamProcedureExecutorDialog.this.lstVariables.getSelectionCount() == 0) {return;}
                BeamProcedureExecutorDialog.this.txtQuery.setText(BeamProcedureExecutorDialog.this.txtQuery.getText() + "${" + BeamProcedureExecutorDialog.this.lstVariables.getSelection()[0] + "}");
                BeamProcedureExecutorDialog.this.lstVariables.setSelection(new String[0]);
                BeamProcedureExecutorDialog.this.txtQuery.forceFocus();
            }

            @Override
            public void widgetDefaultSelected(SelectionEvent selectionEvent) {}
        });

        this.txtQuery = new TextVar( transMeta, shell, SWT.LEFT | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER );
        props.setLook( this.txtQuery, Props.WIDGET_STYLE_FIXED);
        formData = new FormData();
        formData.left = new FormAttachment( this.lstVariables, 0 );
        formData.top = new FormAttachment( lblQuery, margin );
        formData.right = new FormAttachment( 100, 0 );
        formData.bottom = new FormAttachment( lblQuery, 250);
        this.txtQuery.setLayoutData(formData);
        lastControl = this.txtQuery;


        this.wOK = new Button( this.shell, SWT.PUSH );
        this.wOK.setText( BaseMessages.getString( PACKAGE, "System.Button.OK" ) );
        this.wCancel = new Button( this.shell, SWT.PUSH );
        this.wCancel.setText( BaseMessages.getString( PACKAGE, "System.Button.Cancel" ) );
        this.setButtonPositions( new Button[] { this.wOK, this.wCancel }, this.margin, null );


        // Add listeners
        this.lsOK = e -> this.ok();
        this.lsCancel = e -> this.cancel();

        this.wOK.addListener( SWT.Selection, this.lsOK );
        this.wCancel.addListener( SWT.Selection, this.lsCancel );

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected( SelectionEvent e ) {
                BeamProcedureExecutorDialog.this.ok();
            }
        };

        wStepname.addSelectionListener( this.lsDef );
        this.cboDatabaseType.addSelectionListener( this.lsDef );
        this.txtServer.addSelectionListener( this.lsDef );
        this.txtPort.addSelectionListener( this.lsDef );
        this.txtDatabaseName.addSelectionListener( this.lsDef );
        this.txtConnectionString.addSelectionListener( this.lsDef );
        this.txtUsername.addSelectionListener( this.lsDef );
        this.txtPassword.addSelectionListener( this.lsDef );
        this.txtQuery.addSelectionListener( this.lsDef );

        FocusListener focusListener = new FocusListener() {
            @Override public void focusGained(FocusEvent focusEvent) {getConnectionStringView();}
            @Override public void focusLost(FocusEvent focusEvent) {getConnectionStringView();}
        };

        ModifyListener modifyListener = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent modifyEvent) {
                getConnectionStringView();
            }
        };

        cboDatabaseType.addFocusListener(focusListener);
        txtServer.addFocusListener(focusListener);
        txtPort.addFocusListener(focusListener);
        txtDatabaseName.addFocusListener(focusListener);
        txtConnectionString.addModifyListener(modifyListener);

        // Detect X or ALT-F4 or something that kills this window...
        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed( ShellEvent e ) {
                BeamProcedureExecutorDialog.this.cancel();
            }
        });

        this.getData( );
        this.getConnectionStringView();
        setSize();
        this.metadata.setChanged(this.changed);

        this.shell.open();
        while ( !this.shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
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
        helpButton.setText(BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.HelpButton" ));
        helpButton.addListener(SWT.Selection, e ->
                Web.open(this, BaseMessages.getString( PACKAGE, "BeamProcedureExecutor.HelpLink" ))
        );
        return helpButton;
    }

    public void getData( ) {
        this.wStepname.setText( stepname );
        this.cboDatabaseType.setText(Const.NVL(this.metadata.getDatabaseType(), ""));
        this.txtServer.setText(Const.NVL(this.metadata.getServer(), ""));
        this.txtPort.setText(Const.NVL(this.metadata.getPort(), ""));
        this.txtDatabaseName.setText(Const.NVL(this.metadata.getDatabaseName(), ""));
        this.txtConnectionString.setText(Const.NVL(this.metadata.getConnectionString(), ""));
        this.txtUsername.setText(Const.NVL(this.metadata.getUsername(), ""));
        this.txtPassword.setText(Const.NVL(this.metadata.getPassword(), ""));
        this.txtQuery.setText(Const.NVL(this.metadata.getQuery(), ""));
        this.txtConnectionStringView.setText(Const.NVL(this.metadata.getConnectionStringView(), ""));

        this.wStepname.selectAll();
        this.wStepname.setFocus();
    }

    private void cancel() {
        this.stepname = null;
        this.metadata.setChanged(this.changed);
        dispose();
    }

    private void ok() {
        try {
            this.checkFields();
            getInfo(metadata);
            dispose();
        }catch (Exception ex){
            SimpleMessageDialog.openWarning(shell, "Aviso", ex.getMessage());
        }
    }

    private void getInfo(BeamProcedureExecutorMeta metadata) throws Exception {
        stepname = wStepname.getText();
        metadata.setDatabaseType(cboDatabaseType.getText());
        metadata.setServer(txtServer.getText());
        metadata.setPort(txtPort.getText());
        metadata.setDatabaseName(txtDatabaseName.getText());
        metadata.setConnectionString(txtConnectionString.getText());
        metadata.setUsername(txtUsername.getText());
        metadata.setPassword(txtPassword.getText());
        metadata.setQuery(txtQuery.getText());
        metadata.setConnectionStringView(txtConnectionStringView.getText());

        metadata.setChanged();
    }

    private void getConnectionStringView() {
        try {
            if(!txtConnectionString.getText().isEmpty()) {
                txtConnectionStringView.setText(txtConnectionString.getText());
            } else {
                boolean showString = !cboDatabaseType.getText().isEmpty() && !txtServer.getText().isEmpty() && !txtPort.getText().isEmpty() && !txtDatabaseName.getText().isEmpty();

                if(showString) {
                    String connectionUrl = DatabaseUtil.getConnectionUrl(cboDatabaseType.getText(), txtServer.getText(), txtPort.getText(), txtDatabaseName.getText(), txtConnectionString.getText());
                    txtConnectionStringView.setText(connectionUrl);
                }
            }
        } catch (Exception e) {
            SimpleMessageDialog.openWarning(shell, "Aviso", "Erro ao montar a URL de conexao: " + e.getMessage());
        }
    }

    private void checkFields() throws Exception {

        boolean isCustomUrlEmpty = Strings.isNullOrEmpty(txtConnectionString.getText());

        if (Utils.isEmpty(wStepname.getText())) {return;}
        if (Strings.isNullOrEmpty(cboDatabaseType.getText())) {throw new Exception("Tipo de banco de dados nao informado.");}
        if (!BeamDatabaseConnectorHelper.getInstance().getDrivers().containsKey(cboDatabaseType.getText())) {throw new Exception("Tipo de banco de dados invalido.");}
        if (isCustomUrlEmpty && Strings.isNullOrEmpty(txtServer.getText())) {throw new Exception("Servidor nao informado.");}
        if (isCustomUrlEmpty && Strings.isNullOrEmpty(txtPort.getText())) {throw new Exception("Porta nao informada.");}
        if (isCustomUrlEmpty && Strings.isNullOrEmpty(txtDatabaseName.getText())) {throw new Exception("Nome da base de dados nao informado.");}
        if (Strings.isNullOrEmpty(txtUsername.getText())) {throw new Exception("Usuario nao informado.");}
        if (Strings.isNullOrEmpty(txtPassword.getText())) {throw new Exception("Senha nao informado.");}
        if (Strings.isNullOrEmpty(txtQuery.getText())) {throw new Exception("Query nao informado.");}
    }
}
