
package org.kettle.beam.steps.kafka;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.kettle.beam.core.util.Strings;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class BeamProduceDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamProduce.class; // for i18n purposes, needed by Translator2!!
  private final BeamProduceMeta input;

  int middle;
  int margin;

  private TextVar wBootstrapServers;
  private TextVar wTopic;
  private TextVar wKeyField;
  private TextVar wMessageField;

  public BeamProduceDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamProduceMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BeamProduceDialog.DialogTitle" ) );

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

    Label wlBootstrapServers = new Label( shell, SWT.RIGHT );
    wlBootstrapServers.setText( BaseMessages.getString( PKG, "BeamProduceDialog.BootstrapServers" ) );
    props.setLook( wlBootstrapServers );
    FormData fdlBootstrapServers = new FormData();
    fdlBootstrapServers.left = new FormAttachment( 0, 0 );
    fdlBootstrapServers.top = new FormAttachment( lastControl, margin );
    fdlBootstrapServers.right = new FormAttachment( middle, -margin );
    wlBootstrapServers.setLayoutData( fdlBootstrapServers );
    wBootstrapServers = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBootstrapServers );
    FormData fdBootstrapServers = new FormData();
    fdBootstrapServers.left = new FormAttachment( middle, 0 );
    fdBootstrapServers.top = new FormAttachment( wlBootstrapServers, 0, SWT.CENTER );
    fdBootstrapServers.right = new FormAttachment( 100, 0 );
    wBootstrapServers.setLayoutData( fdBootstrapServers );
    lastControl = wBootstrapServers;

    Label wlTopic = new Label( shell, SWT.RIGHT );
    wlTopic.setText( BaseMessages.getString( PKG, "BeamProduceDialog.Topic" ) );
    props.setLook( wlTopic );
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment( 0, 0 );
    fdlTopic.top = new FormAttachment( lastControl, margin );
    fdlTopic.right = new FormAttachment( middle, -margin );
    wlTopic.setLayoutData( fdlTopic );
    wTopic = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTopic );
    FormData fdTopic = new FormData();
    fdTopic.left = new FormAttachment( middle, 0 );
    fdTopic.top = new FormAttachment( wlTopic, 0, SWT.CENTER );
    fdTopic.right = new FormAttachment( 100, 0 );
    wTopic.setLayoutData( fdTopic );
    lastControl = wTopic;

    Label wlKeyField = new Label( shell, SWT.RIGHT );
    wlKeyField.setText( BaseMessages.getString( PKG, "BeamProduceDialog.KeyField" ) );
    props.setLook( wlKeyField );
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment( 0, 0 );
    fdlKeyField.top = new FormAttachment( lastControl, margin );
    fdlKeyField.right = new FormAttachment( middle, -margin );
    wlKeyField.setLayoutData( fdlKeyField );
    wKeyField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wKeyField );
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment( middle, 0 );
    fdKeyField.top = new FormAttachment( wlKeyField, 0, SWT.CENTER );
    fdKeyField.right = new FormAttachment( 100, 0 );
    wKeyField.setLayoutData( fdKeyField );
    lastControl = wKeyField;

    Label wlMessageField = new Label( shell, SWT.RIGHT );
    wlMessageField.setText( BaseMessages.getString( PKG, "BeamProduceDialog.MessageField" ) );
    props.setLook( wlMessageField );
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment( 0, 0 );
    fdlMessageField.top = new FormAttachment( lastControl, margin );
    fdlMessageField.right = new FormAttachment( middle, -margin );
    wlMessageField.setLayoutData( fdlMessageField );
    wMessageField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageField );
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment( middle, 0 );
    fdMessageField.top = new FormAttachment( wlMessageField, 0, SWT.CENTER );
    fdMessageField.right = new FormAttachment( 100, 0 );
    wMessageField.setLayoutData( fdMessageField );
    lastControl = wMessageField;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, lastControl );


    wOK.addListener( SWT.Selection, e -> ok() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wBootstrapServers.addSelectionListener( lsDef );
    wTopic.addSelectionListener( lsDef );
    wKeyField.addSelectionListener( lsDef );
    wMessageField.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addListener( SWT.Close, e->cancel());

    getData();
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
   * Populate the widgets.
   */
  public void getData() {
    wStepname.setText( stepname );
    wBootstrapServers.setText( Const.NVL( input.getBootstrapServers(), "" ) );
    wTopic.setText( Const.NVL( input.getTopic(), "" ) );
    wKeyField.setText( Const.NVL( input.getKeyField(), "" ) );
    wMessageField.setText( Const.NVL( input.getMessageField(), "" ) );

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
      if (Strings.isNullOrEmpty(wBootstrapServers.getText()) ) {throw new Exception("Servidores nao informados.");}
      if (Strings.isNullOrEmpty(wTopic.getText()) ) {throw new Exception("Topico nao informado.");}
      if (Strings.isNullOrEmpty(wKeyField.getText()) ) {throw new Exception("Campo Chave nao informado.");}
      if (Strings.isNullOrEmpty(wMessageField.getText()) ) {throw new Exception("Campo Mensagem nao informado.");}
      getInfo(input);
      dispose();

    }catch (Exception ex){
      SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

    }
  }

  private void getInfo( BeamProduceMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setBootstrapServers( wBootstrapServers.getText() );
    in.setTopic( wTopic.getText() );
    in.setKeyField( wKeyField.getText() );
    in.setMessageField( wMessageField.getText() );

    input.setChanged();
  }
}