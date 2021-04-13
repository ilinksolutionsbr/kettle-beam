
package org.kettle.beam.steps.bq;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.kettle.beam.core.KettleErrorDialog;
import org.kettle.beam.core.fn.BQSchemaAndRecordToKettleFn;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.core.util.Web;
import org.kettle.beam.util.AuthUtil;
import org.kettle.beam.util.BeamConst;
import org.kettle.beam.util.DatabaseUtil;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class BeamBQInputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamBQInputDialog.class; // for i18n purposes, needed by Translator2!!
  private final BeamBQInputMeta input;
  public static final String BIGQUERY_JDBC_DRIVER = "com.simba.googlebigquery.jdbc42.Driver";
  public static final String BIGQUERY_AUTH_URL = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;";

  int middle;
  int margin;

  private TextVar wProjectId;
  private TextVar wDatasetId;
  private TextVar wTableId;
  private TextVar wQuery;
  private TableView wFields;

  public BeamBQInputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamBQInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "BeamBQInputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

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

    Label wlProjectId = new Label( shell, SWT.RIGHT );
    wlProjectId.setText( BaseMessages.getString( PKG, "BeamBQInputDialog.ProjectId" ) );
    props.setLook( wlProjectId );
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment( 0, 0 );
    fdlProjectId.top = new FormAttachment( lastControl, margin );
    fdlProjectId.right = new FormAttachment( middle, -margin );
    wlProjectId.setLayoutData( fdlProjectId );
    wProjectId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wProjectId );
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment( middle, 0 );
    fdProjectId.top = new FormAttachment( wlProjectId, 0, SWT.CENTER );
    fdProjectId.right = new FormAttachment( 100, 0 );
    wProjectId.setLayoutData( fdProjectId );
    lastControl = wProjectId;

    Label wlDatasetId = new Label( shell, SWT.RIGHT );
    wlDatasetId.setText( BaseMessages.getString( PKG, "BeamBQInputDialog.DatasetId" ) );
    props.setLook( wlDatasetId );
    FormData fdlDatasetId = new FormData();
    fdlDatasetId.left = new FormAttachment( 0, 0 );
    fdlDatasetId.top = new FormAttachment( lastControl, margin );
    fdlDatasetId.right = new FormAttachment( middle, -margin );
    wlDatasetId.setLayoutData( fdlDatasetId );
    wDatasetId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDatasetId );
    FormData fdDatasetId = new FormData();
    fdDatasetId.left = new FormAttachment( middle, 0 );
    fdDatasetId.top = new FormAttachment( wlDatasetId, 0, SWT.CENTER );
    fdDatasetId.right = new FormAttachment( 100, 0 );
    wDatasetId.setLayoutData( fdDatasetId );
    lastControl = wDatasetId;

    Label wlTableId = new Label( shell, SWT.RIGHT );
    wlTableId.setText( BaseMessages.getString( PKG, "BeamBQInputDialog.TableId" ) );
    props.setLook( wlTableId );
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment( 0, 0 );
    fdlTableId.top = new FormAttachment( lastControl, margin );
    fdlTableId.right = new FormAttachment( middle, -margin );
    wlTableId.setLayoutData( fdlTableId );
    wTableId = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTableId );
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment( middle, 0 );
    fdTableId.top = new FormAttachment( wlTableId, 0, SWT.CENTER );
    fdTableId.right = new FormAttachment( 100, 0 );
    wTableId.setLayoutData( fdTableId );
    lastControl = wTableId;

    Label wlQuery = new Label( shell, SWT.LEFT );
    wlQuery.setText( BaseMessages.getString( PKG, "BeamBQInputDialog.Query" ) );
    props.setLook( wlQuery );
    FormData fdlQuery = new FormData();
    fdlQuery.left = new FormAttachment( 0, 0 );
    fdlQuery.top = new FormAttachment( lastControl, margin );
    fdlQuery.right = new FormAttachment( 100, 0 );
    wlQuery.setLayoutData( fdlQuery );
    wQuery = new TextVar( transMeta, shell, SWT.LEFT | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wQuery, Props.WIDGET_STYLE_FIXED);
    FormData fdQuery = new FormData();
    fdQuery.left = new FormAttachment( 0, 0 );
    fdQuery.top = new FormAttachment( wlQuery, margin );
    fdQuery.right = new FormAttachment( 100, 0 );
    fdQuery.bottom = new FormAttachment( wlQuery, 250);
    wQuery.setLayoutData( fdQuery );
    lastControl = wQuery;

    Label wlFields = new Label( shell, SWT.LEFT );
    wlFields.setText( BaseMessages.getString( PKG, "BeamBQInputDialog.Fields" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( lastControl, margin );
    fdlFields.right = new FormAttachment( middle, -margin );
    wlFields.setLayoutData( fdlFields );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wGet, wCancel }, margin, null );

    ColumnInfo[] columns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "BeamBQInputDialog.Fields.Column.Name" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "BeamBQInputDialog.Fields.Column.NewName" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "BeamBQInputDialog.Fields.Column.KettleType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), false ),
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
    lsOK = e -> ok();
    lsCancel = e -> cancel();

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, e-> getFields() );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wProjectId.addSelectionListener( lsDef );
    wDatasetId.addSelectionListener( lsDef );
    wTableId.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData( );
    setSize();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
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
    helpButton.setText(BaseMessages.getString( PKG, "BeamBQInputDialog.HelpButton" ));
    helpButton.addListener(SWT.Selection, e ->
            Web.open(this, BaseMessages.getString( PKG, "BeamBQInputDialog.HelpLink" ))
    );
    return helpButton;
  }

  public void getFields() {
    try {

      BeamBQInputMeta meta = new BeamBQInputMeta();
//      getInfo(meta);

      RowMetaInterface rowMeta = new RowMeta();

      if(wQuery.getText().isEmpty()) {
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

        if ( StringUtils.isNotEmpty( wDatasetId.getText() ) &&
                StringUtils.isNotEmpty( wTableId.getText() )) {

          Table table = bigQuery.getTable(
                  transMeta.environmentSubstitute( wDatasetId.getText() ),
                  transMeta.environmentSubstitute( wTableId.getText() )
          );

          TableDefinition definition = table.getDefinition();
          Schema schema = definition.getSchema();

          FieldList fieldList = schema.getFields();
          wFields.clearAll();

          for ( int i = 0; i< fieldList.size(); i++) {
            Field field = fieldList.get( i );

            String name = field.getName();
            String type = field.getType().name();

            int kettleType = BQSchemaAndRecordToKettleFn.AvroType.valueOf( type ).getKettleType();
            rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( name, kettleType ) );
          }
        } else {
          throw new Exception("Devem ser informados os nomes de dataset e tabela para obter os campos");
        }
      } else {
        try {
          //Removendo ponto e vírgula final caso haja
          String sql = wQuery.getText();

          List<String> queryTypes = new ArrayList<>(Arrays.asList("DELETE", "UPDATE", "TRUNCATE"));
          boolean hasOnlySelect = queryTypes.stream().noneMatch(s -> sql.toUpperCase().contains(s));

          if(hasOnlySelect) {
            String connectionString = buildConnectionString();

            //Recuperando a informação dos metadados - nome da coluna e tipo, para criar os retornos
            ResultSetMetaData metadata = DatabaseUtil.executeGetFieldsQuery(sql, BIGQUERY_JDBC_DRIVER, connectionString, null, null);
            wFields.clearAll();

            for (int i = 1; i <= metadata.getColumnCount(); i++){
              int kettleType = BQSchemaAndRecordToKettleFn.AvroType.valueOf( metadata.getColumnTypeName(i) ).getKettleType();
              rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( metadata.getColumnName(i), kettleType ) );
            }
          } else {
            throw new Exception("Só é possível obter campos usando uma consulta quando esta for somente do tipo SELECT");
          }
        } catch (Exception ex) {
          SimpleMessageDialog.openWarning(shell, "Aviso", "Erro encontrado: " + ex.getMessage());
        }
      }

      BaseStepDialog.getFieldsFromPrevious( rowMeta, wFields, 1, new int[] { 1 }, new int[] { 3 }, -1, -1, true, null );
    } catch ( Exception e ) {
      new KettleErrorDialog( shell, "Error", "Error getting BQ fields", e );
    }
  }

  private String buildConnectionString() throws Exception {
    StringBuilder connection = new StringBuilder();
    String configPath = System.getenv(BeamConst.GOOGLE_CREDENTIALS_ENVIRONMENT_VARIABLE);
    ServiceAccountCredentials serviceAccount = (ServiceAccountCredentials) AuthUtil.getCredentials(configPath);

    connection.append(BIGQUERY_AUTH_URL)
              .append("ProjectId=").append(serviceAccount.getProjectId())
              .append(";OAuthType=0;")
              .append("OAuthServiceAcctEmail=").append(serviceAccount.getClientEmail())
              .append(";OAuthPvtKeyPath=").append(configPath).append(";");

    return connection.toString();
  }


  /**
   * Populate the widgets.
   */
  public void getData( ) {
    wStepname.setText( stepname );
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wDatasetId.setText(Const.NVL(input.getDatasetId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));
    wQuery.setText(Const.NVL(input.getQuery(), ""));

    for (int i=0;i<input.getFields().size();i++) {
      BQField field = input.getFields().get( i );
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

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    try {
      if (Utils.isEmpty(wStepname.getText())) {return;}
      if (Strings.isNullOrEmpty(wProjectId.getText()) ) {throw new Exception("Projeto nao informado.");}
      if (Strings.isNullOrEmpty(wDatasetId.getText()) ) {throw new Exception("DataSet nao informado.");}
      if (Strings.isNullOrEmpty(wTableId.getText()) ) {throw new Exception("Tabela nao informada.");}
      getInfo(input);
      dispose();

    }catch (Exception ex){
      SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

    }
  }

  private void getInfo( BeamBQInputMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setProjectId( wProjectId.getText() );
    in.setDatasetId( wDatasetId.getText() );
    in.setTableId( wTableId.getText() );
    in.setQuery( wQuery.getText() );
    in.getFields().clear();
    for (int i=0;i<wFields.nrNonEmpty();i++) {
      TableItem item = wFields.getNonEmpty( i );
      String name = item.getText(1);
      String newName = item.getText(2);
      String kettleType = item.getText(3);
      in.getFields().add(new BQField( name, newName, kettleType ));
    }

    input.setChanged();
  }
}
