package org.kettle.beam.steps.database;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.core.util.Web;
import org.kettle.beam.util.BeamConst;
import org.kettle.beam.util.DatabaseUtil;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
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
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.sql.*;
import java.util.ArrayList;


/**
 * Classe responsável por criar a janela de configuração do step
 * Database Connector.
 *
 * @author Renato Dornelas Cardoso <renato@romaconsulting.com.br>
 */
public class BeamDatabaseConnectorDialog extends BaseStepDialog implements StepDialogInterface {

    //region Attributes

    private static Class<?> PACKAGE = BeamDatabaseConnectorDialog.class;
    private BeamDatabaseConnectorMeta metadata;

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
    private Combo cboQueryType;
    private List lstVariables;
    private Label lblQuery;
    private TextVar txtQuery;
    private TableView tblFields;
    private Label lblFields;
    private Text txtConnectionStringView;

    //endregion

    //region Constructors

    /**
     * Construtor padrão
     *
     * @param parent
     * @param metadata
     * @param transMeta
     * @param sname
     */
    public BeamDatabaseConnectorDialog(Shell parent, Object metadata, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) metadata, transMeta, sname);
        this.metadata = (BeamDatabaseConnectorMeta)metadata;
    }

    //endregion

    //region Methods

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
        this.shell.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.DialogTitle" ) );

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
        lblDatabaseType.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.DatabaseType" ) );
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
        lblServer.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Server" ) );
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
        lblPort.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Port" ) );
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
        lblDbName.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.DatabaseName" ) );
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
        lblConnectionString.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.ConnectionString" ) );
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
        lblUsername.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Username" ) );
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
        lblPassword.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Password" ) );
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


        Label lblQueryType = new Label( shell, SWT.RIGHT );
        lblQueryType.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.QueryType" ) );
        props.setLook(lblQueryType);
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( lastControl, margin );
        formData.right = new FormAttachment( middle, -margin );
        lblQueryType.setLayoutData(formData);
        this.cboQueryType = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( this.cboQueryType );
        this.cboQueryType.setItems( BeamDatabaseConnectorHelper.getInstance().getQueryTypeNames());
        formData = new FormData();
        formData.left = new FormAttachment( middle, 0 );
        formData.top = new FormAttachment( lblQueryType, 0, SWT.CENTER );
        formData.right = new FormAttachment( 100, 0 );
        this.cboQueryType.setLayoutData(formData);
        this.cboQueryType.addModifyListener(this.cboQueryType_Selected());
        lastControl = this.cboQueryType;


        Label lblConnectionStringView = new Label( shell, SWT.RIGHT );
        lblConnectionStringView.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.ConnectionStringView" ) );
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
        lblQuery.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Query" ) );
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
                if (BeamDatabaseConnectorDialog.this.lstVariables.getSelectionCount() == 0) {return;}
                BeamDatabaseConnectorDialog.this.txtQuery.setText(BeamDatabaseConnectorDialog.this.txtQuery.getText() + "${" + BeamDatabaseConnectorDialog.this.lstVariables.getSelection()[0] + "}");
                BeamDatabaseConnectorDialog.this.lstVariables.setSelection(new String[0]);
                BeamDatabaseConnectorDialog.this.txtQuery.forceFocus();
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
        this.wGet = new Button(this.shell, SWT.PUSH);
        this.wGet.setText(BaseMessages.getString( PACKAGE, "System.Button.GetFields" ) );
        this.wCancel = new Button( this.shell, SWT.PUSH );
        this.wCancel.setText( BaseMessages.getString( PACKAGE, "System.Button.Cancel" ) );
        this.setButtonPositions( new Button[] { this.wOK, this.wGet, this.wCancel }, this.margin, null );


        this.lblFields = new Label( shell, SWT.LEFT );
        this.lblFields.setText( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Fields" ) );
        props.setLook(this.lblFields );
        FormData fdlFields = new FormData();
        fdlFields.left = new FormAttachment( 0, 0 );
        fdlFields.top = new FormAttachment( lastControl, margin );
        fdlFields.right = new FormAttachment( middle, -margin );
        this.lblFields.setLayoutData( fdlFields );
        ColumnInfo[] columns = new ColumnInfo[] {
                new ColumnInfo( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Fields.Column.From" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
                new ColumnInfo( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Fields.Column.To" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false ),
                new ColumnInfo( BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.Fields.Column.Type" ), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), false ),
        };
        this.tblFields = new TableView( Variables.getADefaultVariableSpace(), shell, SWT.NONE, columns, this.metadata.getFields().size(), null, props);
        props.setLook( this.tblFields );
        formData = new FormData();
        formData.left = new FormAttachment( 0, 0 );
        formData.top = new FormAttachment( this.lblFields, margin );
        formData.right = new FormAttachment( 100, 0 );
        formData.bottom = new FormAttachment( wOK, -2*margin);
        this.tblFields.setLayoutData(formData);
        lastControl = this.tblFields;


        // Add listeners
        this.lsOK = e -> this.ok();
        this.lsCancel = e -> this.cancel();

        this.wOK.addListener( SWT.Selection, this.lsOK );
        this.wGet.addListener( SWT.Selection, e-> this.getFields() );
        this.wCancel.addListener( SWT.Selection, this.lsCancel );

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected( SelectionEvent e ) {
                BeamDatabaseConnectorDialog.this.ok();
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
        this.cboQueryType.addSelectionListener( this.lsDef );
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
                BeamDatabaseConnectorDialog.this.cancel();
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
        helpButton.setText(BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.HelpButton" ));
        helpButton.addListener(SWT.Selection, e ->
                Web.open(this, BaseMessages.getString( PACKAGE, "BeamDatabaseConnector.HelpLink" ))
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
        this.cboQueryType.setText(Const.NVL(this.metadata.getQueryType(), ""));
        this.txtQuery.setText(Const.NVL(this.metadata.getQuery(), ""));
        this.txtConnectionStringView.setText(Const.NVL(this.metadata.getConnectionStringView(), ""));

        FieldInfo field;
        for (int i=0;i<this.metadata.getFields().size();i++) {
            field = this.metadata.getFields().get( i );
            TableItem item = this.tblFields.table.getItem( i );
            item.setText( 1, Const.NVL(field.getColumn(), "") );
            item.setText( 2, Const.NVL(field.getVariable(), "") );
            item.setText( 3, Const.NVL(field.getType(), "") );
        }
        this.tblFields.removeEmptyRows();
        this.tblFields.setRowNums();
        this.tblFields.optWidth( true );

        this.wStepname.selectAll();
        this.wStepname.setFocus();
    }

    private ModifyListener cboQueryType_Selected(){
        return modifyEvent -> {
            Boolean visible = BeamDatabaseConnectorHelper.QUERY_TYPE_SELECT.equalsIgnoreCase(cboQueryType.getText());
            tblFields.setVisible(visible);
            lblFields.setVisible(visible);

            FormData formData;
            formData = new FormData();
            formData.left = new FormAttachment( 0, 0 );
            formData.top = new FormAttachment( lblQuery, margin );
            if(!visible){
                formData.bottom = new FormAttachment( wOK, -2*margin);
            }else{
                formData.bottom = new FormAttachment( lblQuery, 250);
            }
            formData.width = 120;
            lstVariables.setLayoutData(formData);

            formData = new FormData();
            formData.left = new FormAttachment( lstVariables, 0 );
            formData.top = new FormAttachment( lblQuery, margin );
            formData.right = new FormAttachment( 100, 0 );
            if(!visible){
                formData.bottom = new FormAttachment( wOK, -2*margin);
            }else{
                formData.bottom = new FormAttachment( lblQuery, 250);
            }
            txtQuery.setLayoutData(formData);

            txtQuery.getParent().layout();
        };
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

    private void getInfo(BeamDatabaseConnectorMeta metadata) throws Exception {
        stepname = wStepname.getText();
        metadata.setDatabaseType(cboDatabaseType.getText());
        metadata.setServer(txtServer.getText());
        metadata.setPort(txtPort.getText());
        metadata.setDatabaseName(txtDatabaseName.getText());
        metadata.setConnectionString(txtConnectionString.getText());
        metadata.setUsername(txtUsername.getText());
        metadata.setPassword(txtPassword.getText());
        metadata.setQueryType(cboQueryType.getText());
        metadata.setQuery(txtQuery.getText());
        metadata.setConnectionStringView(txtConnectionStringView.getText());
        metadata.getFields().clear();

        if(BeamDatabaseConnectorHelper.QUERY_TYPE_SELECT.equalsIgnoreCase(cboQueryType.getText())) {
            String column;
            String variable;
            String type;
            for (int i = 0; i < tblFields.nrNonEmpty(); i++) {
                TableItem item = tblFields.getNonEmpty(i);
                column = item.getText(1);
                if(!Strings.isNullOrEmpty(column)) {
                    variable = item.getText(2);
                    type = Const.NVL(item.getText(3), "String");
                    metadata.getFields().add(new FieldInfo(column, variable, type));
                }
            }
        }

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

    public void getFields() {

        boolean queryIsSelect = false;

        try{
            this.checkFields();
            if(cboQueryType.getText().equalsIgnoreCase(BeamDatabaseConnectorHelper.QUERY_TYPE_SELECT)){
                queryIsSelect = true;
            } else {
                throw new Exception("Só é necessário Obter Campos quando o Tipo selecionado for " + BeamDatabaseConnectorHelper.QUERY_TYPE_SELECT);
            }
        } catch (Exception ex){
            SimpleMessageDialog.openWarning(shell, "Aviso", ex.getMessage());
        }

        if(queryIsSelect){
            try {
                //Removendo ponto e vírgula final caso haja
                String sql = txtQuery.getText();

                String connectionUrl = DatabaseUtil.getConnectionUrl(cboDatabaseType.getText(), txtServer.getText(), txtPort.getText(), txtDatabaseName.getText(), txtConnectionString.getText());

                ResultSet result = this.executeGetFieldsQuery(sql, cboDatabaseType.getText(), connectionUrl, txtUsername.getText(), txtPassword.getText());

                //Recuperando a informação dos metadados - nome da coluna e tipo, para criar os retornos
                ResultSetMetaData metadata = result.getMetaData();

                RowMetaInterface rowMeta = new RowMeta();
                tblFields.clearAll();

                for (int i = 1; i <= metadata.getColumnCount(); i++){
                    ValueMetaInterface typeName = BeamConst.createValueMeta(metadata.getColumnName(i), metadata.getColumnType(i));
                    rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( metadata.getColumnName(i), typeName.getType() ) );
                }

                BaseStepDialog.getFieldsFromPrevious( rowMeta, tblFields, 1, new int[] { 1 }, new int[] { 3 }, -1, -1, true, null );
            } catch (Exception ex) {
                SimpleMessageDialog.openWarning(shell, "Aviso", "Erro encontrado: " + ex.getMessage());
            }
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
        if (Strings.isNullOrEmpty(cboQueryType.getText())) {throw new Exception("Tipo de query nao informado.");}
        if (!BeamDatabaseConnectorHelper.getInstance().getQueryTypes().containsKey(cboQueryType.getText())) {throw new Exception("Tipo de query invalido.");}
        if (Strings.isNullOrEmpty(txtQuery.getText())) {throw new Exception("Query nao informado.");}
    }

    public ResultSet executeGetFieldsQuery(String sql, String databaseType, String connectionString, String username, String password) throws SQLException, ClassNotFoundException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String driver = BeamDatabaseConnectorHelper.getInstance().getDriver(databaseType);

        java.util.List<String> parameters = new ArrayList<>();
        sql = DatabaseUtil.prepareSQL(sql, parameters);

        Class.forName(driver);
        connection = DriverManager.getConnection(connectionString, username, password);
        connection.setAutoCommit(false);
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setMaxRows(1);

        //iNSERINDO PARÂMETROS NULOS CASO EXISTAM, APENAS PARA RECEBER OS METADADOS DO RESULTADO
        if(parameters.size() > 0) {
            for(int i = 0; i<parameters.size(); i++){
                preparedStatement.setNull(i+1, Types.VARCHAR);
            }

        }

        ResultSet resultSet = preparedStatement.executeQuery();

        //Rollback para que não ocorra problemas
        connection.rollback();

        return resultSet;
    }

    //endregion

}
