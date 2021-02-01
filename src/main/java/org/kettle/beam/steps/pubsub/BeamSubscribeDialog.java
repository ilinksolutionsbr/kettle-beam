
package org.kettle.beam.steps.pubsub;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.kettle.beam.core.BeamDefaults;
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

public class BeamSubscribeDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BeamSubscribe.class; // for i18n purposes, needed by Translator2!!
  private final BeamSubscribeMeta input;

  int middle;
  int margin;

  private TextVar wSubscription;
  private TextVar wTopic;
  private Combo wMessageType;
  private TextVar wMessageField;

  public BeamSubscribeDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (BeamSubscribeMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "BeamSubscribeDialog.DialogTitle" ) );

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

    Label wlSubscription = new Label( shell, SWT.RIGHT );
    wlSubscription.setText( BaseMessages.getString( PKG, "BeamSubscribeDialog.Subscription" ) );
    props.setLook( wlSubscription );
    FormData fdlSubscription = new FormData();
    fdlSubscription.left = new FormAttachment( 0, 0 );
    fdlSubscription.top = new FormAttachment( lastControl, margin );
    fdlSubscription.right = new FormAttachment( middle, -margin );
    wlSubscription.setLayoutData( fdlSubscription );
    wSubscription = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSubscription );
    FormData fdSubscription = new FormData();
    fdSubscription.left = new FormAttachment( middle, 0 );
    fdSubscription.top = new FormAttachment( wlSubscription, 0, SWT.CENTER );
    fdSubscription.right = new FormAttachment( 100, 0 );
    wSubscription.setLayoutData( fdSubscription );
    lastControl = wSubscription;
    
    Label wlTopic = new Label( shell, SWT.RIGHT );
    wlTopic.setText( BaseMessages.getString( PKG, "BeamSubscribeDialog.Topic" ) );
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

    Label wlMessageType = new Label( shell, SWT.RIGHT );
    wlMessageType.setText( BaseMessages.getString( PKG, "BeamSubscribeDialog.MessageType" ) );
    props.setLook( wlMessageType );
    FormData fdlMessageType = new FormData();
    fdlMessageType.left = new FormAttachment( 0, 0 );
    fdlMessageType.top = new FormAttachment( lastControl, margin );
    fdlMessageType.right = new FormAttachment( middle, -margin );
    wlMessageType.setLayoutData( fdlMessageType );
    wMessageType = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageType );
    wMessageType.setItems( BeamDefaults.PUBSUB_MESSAGE_TYPES );
    FormData fdMessageType = new FormData();
    fdMessageType.left = new FormAttachment( middle, 0 );
    fdMessageType.top = new FormAttachment( wlMessageType, 0, SWT.CENTER );
    fdMessageType.right = new FormAttachment( 100, 0 );
    wMessageType.setLayoutData( fdMessageType );
    lastControl = wMessageType;

    Label wlMessageField = new Label( shell, SWT.RIGHT );
    wlMessageField.setText( BaseMessages.getString( PKG, "BeamSubscribeDialog.MessageField" ) );
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
    wMessageType.addSelectionListener( lsDef );
    wTopic.addSelectionListener( lsDef );

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
    wSubscription.setText( Const.NVL( input.getSubscription(), "" ) );
    wTopic.setText( Const.NVL( input.getTopic(), "" ) );
    wMessageType.setText( Const.NVL( input.getMessageType(), "" ) );
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
      if (Strings.isNullOrEmpty(wSubscription.getText()) ) {throw new Exception("Subscricao nao informado.");}
      if (Strings.isNullOrEmpty(wTopic.getText()) ) {throw new Exception("Topico nao informado.");}
      if (Strings.isNullOrEmpty(wMessageType.getText()) ) {throw new Exception("Tipo de mensagem nao informado.");}
      if (Strings.isNullOrEmpty(wMessageField.getText()) ) {throw new Exception("Campo Mensagem nao informado.");}
      getInfo(input);
      dispose();

    }catch (Exception ex){
      SimpleMessageDialog.openWarning(this.shell, "Aviso", ex.getMessage());

    }
  }

  private void getInfo( BeamSubscribeMeta in ) {
    stepname = wStepname.getText(); // return value

    in.setSubscription( wSubscription.getText() );
    in.setTopic( wTopic.getText() );
    in.setMessageType( wMessageType.getText() );
    in.setMessageField( wMessageField.getText() );

    input.setChanged();
  }
}