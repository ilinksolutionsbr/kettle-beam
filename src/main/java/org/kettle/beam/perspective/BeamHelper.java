/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.kettle.beam.perspective;

import org.apache.commons.lang.StringUtils;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.kettle.beam.core.KettleErrorDialog;
import org.kettle.beam.core.metastore.SerializableMetaStore;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.job.pipeline.JobPipeline;
import org.kettle.beam.metastore.*;
import org.kettle.beam.pipeline.KettleBeamPipelineExecutor;
import org.kettle.beam.pipeline.fatjar.FatJarBuilder;
import org.kettle.beam.steps.runners.RunnerParameter;
import org.kettle.beam.steps.runners.dataflow.BeamDataflowRunnerMeta;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.KettleURLClassLoader;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.SimpleMessageDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class BeamHelper extends AbstractXulEventHandler implements ISpoonMenuController {
  protected static Class<?> PKG = BeamHelper.class; // for i18n
  protected LogChannelInterface log;
  private static BeamHelper instance = null;

  public static final String TRANSFORMATION_RUNNER_PREFIX = "Transformation: ";
  public static final String METASTORE_PREFIX = "Local: ";

  private Spoon spoon;

  private BeamHelper() {
    spoon = Spoon.getInstance();
  }

  public static BeamHelper getInstance() {
    if ( instance == null ) {
      instance = new BeamHelper(); ;
      instance.spoon.addSpoonMenuController( instance );
    }
    return instance;
  }

  @Override public void updateMenu( Document doc ) {

  }

  public String getName() {
    return "beamHelper";
  }


  public void runBeam() {

    TransMeta transMeta = spoon.getActiveTransformation();
    if ( transMeta == null ) {
      showMessage( "Sorry", "The Apache Beam implementation works with transformations only." );
      return;
    }

    // Getting runner steps from the transformation
    List<StepMeta> runnerSteps = new ArrayList<>();
    for(StepMeta stepMeta: transMeta.getSteps()) {
      if(stepMeta.getStepID().toLowerCase().contains("runner")) {
        runnerSteps.add(stepMeta);
      }
    }

    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> namesOriginal = factory.getElementNames();
      List<String> elementNames = new ArrayList<>();
      for(String name: namesOriginal) {
        elementNames.add(METASTORE_PREFIX + name);
      }
      if(!runnerSteps.isEmpty()) {
        runnerSteps.forEach(s -> elementNames.add(TRANSFORMATION_RUNNER_PREFIX + s.getName()));
      }
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectBeamJobConfigToRun.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectBeamJobConfigToRun.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        BeamJobConfig config;
        if(choice.toLowerCase().contains(TRANSFORMATION_RUNNER_PREFIX.toLowerCase())) {
          config = parseJobConfig(choice, runnerSteps);
        } else {
          choice = choice.split(METASTORE_PREFIX)[1];
          config = factory.loadElement( choice );
        }

        Runnable runnable = new Runnable() {
          @Override public void run() {

            try {
              // KettleURLClassLoader kettleURLClassLoader = createKettleURLClassLoader(
              //  BeamConst.findLibraryFilesToStage( null, transMeta.environmentSubstitute( config.getPluginsToStage() ), true, true )
              //);

              ClassLoader pluginClassLoader = BeamHelper.this.getClass().getClassLoader();
              KettleBeamPipelineExecutor executor = new KettleBeamPipelineExecutor( spoon.getLog(), transMeta, config, spoon.getMetaStore(), pluginClassLoader );
              executor.execute();
            } catch ( Exception e ) {
              spoon.getDisplay().asyncExec( new Runnable() {
                @Override public void run() {
                  new KettleErrorDialog( spoon.getShell(), "Error", "There was an error building or executing the pipeline. Use the 'Details' button for more information:", e );
                }
              } );
            }
          }
        };

        // Create a new thread on the class loader
        //
        Thread thread = new Thread(runnable);
        thread.start();

        showMessage( "Transformation started",
          "Your transformation was started with the selected Beam Runner." + Const.CR +
            "Now check the spoon console logging for execution feedback and metrics on the various steps." + Const.CR +
            "Not all steps are supported, check the project READ.me and Wiki for up-to-date information" + Const.CR + Const.CR +
            "Enjoy Kettle!"
        );

        TransGraph transGraph = spoon.getActiveTransGraph();
        if ( !transGraph.isExecutionResultsPaneVisible() ) {
          transGraph.showExecutionResults();
          CTabItem transLogTab = transGraph.transLogDelegate.getTransLogTab();
          transLogTab.getParent().setSelection( transLogTab );
        }


      }
    } catch ( Exception e ) {
      new KettleErrorDialog( spoon.getShell(),
        BaseMessages.getString( PKG, "BeamHelper.ErrorRunningTransOnBeam.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.ErrorRunningTransOnBeam.Message" ),
         e
      );
    }

  }

  private BeamJobConfig parseJobConfig(String choice, List<StepMeta> stepsList) {
    BeamJobConfig jobConfig = new BeamJobConfig();

    String stepName = choice.split(TRANSFORMATION_RUNNER_PREFIX)[1];
    StepMeta stepMeta = StepMeta.findStep(stepsList, stepName);
    if(stepMeta.getStepID().equalsIgnoreCase(BeamConst.STRING_BEAM_DATAFLOW_RUNNER_PLUGIN_ID)) {
      BeamDataflowRunnerMeta meta = (BeamDataflowRunnerMeta) stepMeta.getStepMetaInterface();
      jobConfig.setName(meta.getName());
      jobConfig.setRunnerTypeName(RunnerType.DataFlow.name());
      jobConfig.setUserAgent(meta.getUserAgent());
      jobConfig.setDescription("Config from transformation");
      jobConfig.setTempLocation(meta.getTempLocation());
      jobConfig.setPluginsToStage(meta.getPluginsToStage());
      jobConfig.setStepPluginClasses(meta.getStepPluginClasses());
      jobConfig.setStreamingKettleStepsFlushInterval(meta.getStreamingKettleStepsFlushInterval());
      jobConfig.setFatJar(meta.getFatJar());

      jobConfig.setGcpProjectId(meta.getGcpProjectId());
      jobConfig.setGcpAppName(meta.getGcpAppName());
      jobConfig.setGcpStagingLocation(meta.getGcpStagingLocation());
      jobConfig.setGcpTemplateLocation(meta.getGcpTemplateLocation());
      jobConfig.setGcpNetwork(meta.getGcpNetwork());
      jobConfig.setGcpSubNetwork(meta.getGcpSubNetwork());
      jobConfig.setGcpInitialNumberOfWorkers(meta.getGcpInitialNumberOfWorkers());
      jobConfig.setGcpMaximumNumberOfWokers(meta.getGcpMaximumNumberOfWokers());
      jobConfig.setGcpAutoScalingAlgorithm(meta.getGcpAutoScalingAlgorithm());
      jobConfig.setGcpWorkerMachineType(meta.getGcpWorkerMachineType());
      jobConfig.setGcpWorkerDiskType(meta.getGcpWorkerDiskType());
      jobConfig.setGcpDiskSizeGb(meta.getGcpDiskSizeGb());
      jobConfig.setGcpRegion(meta.getGcpRegion());
      jobConfig.setGcpZone(meta.getGcpZone());
      jobConfig.setGcpStreaming(meta.isGcpStreaming());

      List<RunnerParameter> parameters = meta.getParameters();
      for(RunnerParameter parameter: parameters) {
        jobConfig.getParameters().add(new JobParameter(parameter.getVariable(), parameter.getValue()));
      }
    }
    return jobConfig;
  }

  private KettleURLClassLoader createKettleURLClassLoader(List<String> jarFilenames) throws MalformedURLException {

    URL[] urls = new URL[jarFilenames.size()];
    for (int i=0;i<urls.length;i++) {
      urls[i] = new File( jarFilenames.get(i) ).toURI().toURL();
    }
    return new KettleURLClassLoader( urls, ClassLoader.getSystemClassLoader() );
  }

  public void showMessage( String title, String message ) {

    MessageBox messageBox = new MessageBox( spoon.getShell(), SWT.ICON_INFORMATION | SWT.CLOSE );
    messageBox.setText( title );
    messageBox.setMessage( message );
    messageBox.open();
  }

  public void createFileDefinition() {

    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    FileDefinition fileDefinition = new FileDefinition();
    fileDefinition.setName( "My File Definition" );
    boolean ok = false;
    while ( !ok ) {
      FileDefinitionDialog dialog = new FileDefinitionDialog( spoon.getShell(), fileDefinition );
      if ( dialog.open() ) {
        // write to metastore...
        try {
          if ( factory.loadElement( fileDefinition.getName() ) != null ) {
            MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "BeamHelper.Error.FileDefintionExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "BeamHelper.Error.FileDefintionExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( fileDefinition );
              ok = true;
            }
          } else {
            factory.saveElement( fileDefinition );
            ok = true;
          }
        } catch ( Exception exception ) {
          new KettleErrorDialog( spoon.getShell(),
            BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingDefinition.Title" ),
            BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingDefinition.Message" ),
            exception );
          return;
        }
      } else {
        // Cancel
        return;
      }
    }
  }

  public void editFileDefinition() {
    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectDefinitionToEdit.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectDefinitionToEdit.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        FileDefinition fileDefinition = factory.loadElement( choice );

        FileDefinitionDialog dialog = new FileDefinitionDialog( spoon.getShell(), fileDefinition );
        if ( dialog.open() ) {
          // write to metastore...
          try {
            factory.saveElement( fileDefinition );
          } catch ( Exception exception ) {
            new KettleErrorDialog( spoon.getShell(),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingDefinition.Title" ),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingDefinition.Message" ) ,
              exception );
            return;
          }
        }
      }
    } catch ( Exception e ) {
      new KettleErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.ErrorEditingDefinition.Message" ), e );
    }
  }

  public void deleteFileDefinition() {
    MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectDefinitionToDelete.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectDefinitionToDelete.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO | SWT.ICON_ERROR );
        box.setText( BaseMessages.getString( PKG, "BeamHelper.DeleteDefinitionConfirmation.Title" ) );
        box.setMessage( BaseMessages.getString( PKG, "BeamHelper.DeleteDefinitionConfirmation.Message", choice ) );
        int answer = box.open();
        if ( ( answer & SWT.YES ) != 0 ) {
          try {
            factory.deleteElement( choice );
          } catch ( Exception exception ) {
            new KettleErrorDialog( spoon.getShell(),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingDefinition.Title" ),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingDefinition.Message", choice ),
              exception );
          }
        }
      }
    } catch ( Exception e ) {
      new KettleErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingDefinition.Message" ), e );
    }
  }


  public void createBeamJobConfig() {

    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    BeamJobConfig config = new BeamJobConfig();
    config.setName( "My Beam Job Config" );
    boolean ok = false;
    while ( !ok ) {
      BeamJobConfigDialog dialog = new BeamJobConfigDialog( spoon.getShell(), config );
      if ( dialog.open() ) {
        // write to metastore...
        try {
          if ( factory.loadElement( config.getName() ) != null ) {
            MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "BeamHelper.Error.BeamJobConfigExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "BeamHelper.Error.BeamJobConfigExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( config );
              ok = true;
            }
          } else {
            factory.saveElement( config );
            ok = true;
          }
        } catch ( Exception exception ) {
          new KettleErrorDialog( spoon.getShell(),
            BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingJobConfig.Title" ),
            BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingJobConfig.Message" ),
            exception );
          return;
        }
      } else {
        // Cancel
        return;
      }
    }
  }

  public void editBeamJobConfig() {
    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigToEdit.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigToEdit.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        BeamJobConfig beamJobConfig = factory.loadElement( choice );

        BeamJobConfigDialog dialog = new BeamJobConfigDialog( spoon.getShell(), beamJobConfig );
        if ( dialog.open() ) {
          // write to metastore...
          try {
            factory.saveElement( beamJobConfig );
          } catch ( Exception exception ) {
            new KettleErrorDialog( spoon.getShell(),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingJobConfig.Title" ),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorSavingJobConfig.Message" ),
              exception );
            return;
          }
        }
      }
    } catch ( Exception e ) {
      new KettleErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.ErrorEditingJobConfig.Message" ), e );
    }
  }

  public void deleteBeamJobConfig() {
    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigToDelete.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigToDelete.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO | SWT.ICON_ERROR );
        box.setText( BaseMessages.getString( PKG, "BeamHelper.DeleteJobConfigConfirmation.Title" ) );
        box.setMessage( BaseMessages.getString( PKG, "BeamHelper.DeleteJobConfigConfirmation.Message", choice ) );
        int answer = box.open();
        if ( ( answer & SWT.YES ) != 0 ) {
          try {
            factory.deleteElement( choice );
          } catch ( Exception exception ) {
            new KettleErrorDialog( spoon.getShell(),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingJobConfig.Title" ),
              BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingJobConfig.Message", choice ),
              exception );
          }
        }
      }
    } catch ( Exception e ) {
      new KettleErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.Error.ErrorDeletingJobConfig.Message" ), e );
    }
  }

  public void generateFatJar() {

    final Shell shell = Spoon.getInstance().getShell();

    // Ask the use for which Beam Job Config this is.
    //
    MetaStoreFactory<BeamJobConfig> factory = new MetaStoreFactory<>( BeamJobConfig.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE );
    VariableSpace space = Variables.getADefaultVariableSpace();
    String pluginFolders;
    String filename;

    try {
      List<String> elementNames = factory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog selectionDialog = new EnterSelectionDialog( spoon.getShell(), names,
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigForFatJar.Title" ),
        BaseMessages.getString( PKG, "BeamHelper.SelectJobConfigForFatJar.Message" )
      );
      String choice = selectionDialog.open();
      if ( choice != null ) {

        BeamJobConfig jobConfig = factory.loadElement( choice );
        pluginFolders = jobConfig.getPluginsToStage();

        FileDialog dialog = new FileDialog( shell, SWT.SAVE );
        dialog.setText( "Select the location of the Kettle+Beam+Plugins fat jar" );
        dialog.setFilterNames( new String[] { "Jar files (*.jar)", "All Files (*.*)" } );
        dialog.setFilterExtensions( new String[] { "*.jar", "*.*" } ); // Windows
        if ( StringUtils.isNotEmpty( jobConfig.getFatJar() ) ) {
          dialog.setFileName( space.environmentSubstitute( jobConfig.getFatJar() ) );
        }
        filename = dialog.open();
        if ( StringUtils.isEmpty( filename ) ) {
          return;
        }

        IRunnableWithProgress op = new IRunnableWithProgress() {
          public void run( IProgressMonitor monitor ) throws InvocationTargetException, InterruptedException {
            try {

              List<String> files = BeamConst.findLibraryFilesToStage( null, jobConfig.getPluginsToStage(), true, true );
              files.removeIf( s -> s.contains( "commons-logging" ) || s.contains( "log4j" ) || s.contains( "xml-apis" ) );

              // Find the plugin classes for the specified plugins...
              //
              String stepPluginClasses = FatJarBuilder.findPluginClasses( Step.class.getName(), pluginFolders );
              if (StringUtils.isNotEmpty(jobConfig.getStepPluginClasses())) {
                if (StringUtils.isEmpty( stepPluginClasses )) {
                  stepPluginClasses="";
                } else {
                  stepPluginClasses+=",";
                }
                stepPluginClasses+=jobConfig.getStepPluginClasses();
              }
              String xpPluginClasses = FatJarBuilder.findPluginClasses( ExtensionPoint.class.getName(), pluginFolders );
              if (StringUtils.isNotEmpty(jobConfig.getXpPluginClasses())) {
                if (StringUtils.isEmpty( xpPluginClasses )) {
                  xpPluginClasses="";
                } else {
                  xpPluginClasses+=",";
                }
                xpPluginClasses+=jobConfig.getStepPluginClasses();
              }

              FatJarBuilder fatJarBuilder = new FatJarBuilder( filename, files );
              fatJarBuilder.setExtraStepPluginClasses( stepPluginClasses );
              fatJarBuilder.setExtraXpPluginClasses( xpPluginClasses );
              fatJarBuilder.buildTargetJar();

            } catch ( Exception e ) {
              throw new InvocationTargetException( e, "Error building fat jar: " + e.getMessage() );
            }
          }
        };

        ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
        pmd.run( true, true, op );

        MessageBox box = new MessageBox( shell, SWT.CLOSE | SWT.ICON_INFORMATION );
        box.setText( "Fat jar created" );
        box.setMessage( "A fat jar was successfully created : " + filename + Const.CR + "Included plugin folders: " + pluginFolders );
        box.open();
      }
    } catch(Exception e) {
      new KettleErrorDialog( shell, "Error", "Error creating fat jar", e );
    }

  }

  public void exportMetaStore() {
    final Shell shell = Spoon.getInstance().getShell();
    final IMetaStore metaStore = Spoon.getInstance().getMetaStore();

    try {
      FileDialog dialog = new FileDialog(shell, SWT.SAVE);
      dialog.setText("Selecione o local do Metastore");
      dialog.setFilterNames(new String[]{"JSON files (*.json)", "All Files (*.*)"});
      dialog.setFilterExtensions(new String[]{"*.json", "*.*"}); // Windows
      dialog.setFileName("metastore.json");

      String filename = dialog.open();
      if (Strings.isNullOrEmpty(filename)) {
        return;
      }

      SerializableMetaStore sms = new SerializableMetaStore( metaStore );
      FileOutputStream fos = new FileOutputStream( filename );
      fos.write( sms.toJson().getBytes( "UTF-8" ));
      fos.flush();
      fos.close();

      MessageBox box = new MessageBox( shell, SWT.CLOSE | SWT.ICON_INFORMATION );
      box.setText( "Metastore" );
      box.setMessage( "Metastore exportado com sucesso em: "+filename);
      box.open();

    } catch(Exception e) {
      new KettleErrorDialog( shell, "Error", "Ocorreu um erro ao exportar o metastore.", e );
    }

  }

  private void setCurrentStepBeamFlag(String key, String value) {
    TransGraph transGraph = spoon.getActiveTransGraph();
    if (transGraph==null) {
      return;
    }
    StepMeta stepMeta = transGraph.getCurrentStep();
    if (stepMeta==null) {
      return;
    }
    stepMeta.setAttribute(BeamConst.STRING_KETTLE_BEAM, key, value);
    transGraph.redraw();
  }

  public void setBatching() {
    setCurrentStepBeamFlag(BeamConst.STRING_STEP_FLAG_BATCH, "true");
  }

  public void clearBatching() {
    setCurrentStepBeamFlag(BeamConst.STRING_STEP_FLAG_BATCH, "false");
  }

  public void setSingleThreaded() {
    setCurrentStepBeamFlag(BeamConst.STRING_STEP_FLAG_SINGLE_THREADED, "true");
  }

  public void clearSingleThreaded() {
    setCurrentStepBeamFlag(BeamConst.STRING_STEP_FLAG_SINGLE_THREADED, "false");
  }


  public void generateWorkflow(){

    final Shell shell = Spoon.getInstance().getShell();

    JobMeta jobMeta = spoon.getActiveJob();
    if (jobMeta == null) {
      showMessage("Aviso", "Para gerar/publicar o YAML do GCP Workflow, precisa estar com o JOB aberto e em exibição na tela.");
      return;
    }

    try{
      FileDialog dialog = new FileDialog(shell, SWT.SAVE);
      dialog.setText("Selecione o local do YAML.");
      dialog.setFilterNames(new String[]{"YAML files (*.yaml)", "All Files (*.*)"});
      dialog.setFilterExtensions(new String[]{"*.yaml", "*.*"}); // Windows
      dialog.setFileName(jobMeta.getName() + ".yaml");

      String filename = dialog.open();
      if (Strings.isNullOrEmpty(filename)) {
        return;
      }

      spoon.getLog().setLogLevel(LogLevel.DEBUG);
      spoon.showExecutionResults();

      spoon.getLog().logBasic("Gerando GCP Workflow YAML em: " + filename);

      JobPipeline pipeline = new JobPipeline(jobMeta);
      String yaml = pipeline.toGcpYAML();

      FileOutputStream fileOutputStream = new FileOutputStream(filename);
      fileOutputStream.write(yaml.getBytes("UTF-8"));
      fileOutputStream.flush();
      fileOutputStream.close();

      spoon.getLog().logBasic("GCP Workflow YAML gerado com sucesso.");
      showMessage("Sucesso", "YAML gerado com sucesso.");

      MessageBox messageBox = new MessageBox(shell, SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      messageBox.setMessage("Deseja publica o YAML gerado agora ?");
      messageBox.setText("Confirmacao");
      if (messageBox.open() == SWT.YES){
        new Thread(() -> {
          publishWorkflow(jobMeta.getName(), filename);
        }).start();
      }

    }catch(Exception ex){
      spoon.getLog().logError("Ocorreu um erro ao gerar o GCP Workflow YAML.", ex);
      new KettleErrorDialog( spoon.getShell(), "Error", BaseMessages.getString( PKG, "BeamHelper.ErrorEditingJobConfig.Message" ), ex );

    }

  }

  private void publishWorkflow(String name, String source){
    String OS = System.getProperty("os.name").toLowerCase();
    String gcloud = OS.contains("win") ? "gcloud.cmd" : "gcloud.sh";
    String fileGoogleApplicationCredentials;
    String command;
    int result;

    try{

      //SET AUTH
      spoon.getLog().logBasic("Setando o arquivo de configuração do projeto.");
      fileGoogleApplicationCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
      if(Strings.isNullOrEmpty(fileGoogleApplicationCredentials)){fileGoogleApplicationCredentials = System.getProperty("GOOGLE_APPLICATION_CREDENTIALS");}
      if(Strings.isNullOrEmpty(fileGoogleApplicationCredentials)){throw new Exception("Arquivo de Credenciais do Google não localizado.");}
      command = gcloud + " auth activate-service-account --key-file=\"" + fileGoogleApplicationCredentials + "\"";
      result = this.execute(command,"Setando credenciais 'service account'.");

      if(result == 0){

        //SET Project
        JSONParser parser = new JSONParser();
        Object instance = parser.parse(new FileReader(fileGoogleApplicationCredentials));
        JSONObject jsonObject = (JSONObject)instance;
        String projectId = (String)jsonObject.get("project_id");
        if(Strings.isNullOrEmpty(projectId)){throw new Exception("Project Id não encontrado no arquivo de credencial '" + fileGoogleApplicationCredentials + "'");}
        command = gcloud + " config set project " + projectId;
        result = this.execute(command,"Setando projeto '" + projectId + "'");

        if(result == 0){

          //PUBLISH Workflow
          command = gcloud + " beta workflows deploy " + name + " --source=\"" + source + "\"";
          result = this.execute(command,"Publicando GCP Workflow YAML '" + name + "'");

          if(result == 0) {
            spoon.getDisplay().syncExec(()->{
              SimpleMessageDialog.openInformation(spoon.getShell(), "Aviso", "GCP Workflow YAML publicado com sucesso.");
            });

          }else{
            throw new Exception("Ocorreu um erro ao publicar o GCP Workflow YAML.");

          }

        } else{
          throw new Exception("Não foi possivel setar o projeto.");

        }

      }else{
        throw new Exception("Não foi possivel setar o arquivo de credenciais 'service account'.");

      }

    }catch (Exception ex){
      spoon.getLog().logError(ex.getMessage());
      spoon.getDisplay().syncExec(()-> {
        new KettleErrorDialog(spoon.getShell(), "Error", "Erro ao publicar o YAML", ex);
      });

    }
  }

  private int execute(String command, String description) throws IOException, InterruptedException {
    Process process;
    Runtime run = Runtime.getRuntime();
    BufferedReader bufferedReader;
    BufferedReader errorBufferedReader;
    String line;
    StringBuilder builder = new StringBuilder();

    spoon.getLog().logBasic(description);
    process = run.exec(command);
    bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    line = "";
    while ((line=bufferedReader.readLine())!=null) {
      spoon.getLog().logBasic(line);
    }
    errorBufferedReader = new BufferedReader((new InputStreamReader(process.getErrorStream())));
    line = "";
    while ((line=errorBufferedReader.readLine())!=null) {
      builder.append(line + "\n");
    }

    int result = process.waitFor();

    if(builder.length() > 0) {
      if (result == 0) {
        spoon.getLog().logBasic(builder.toString());
      } else {
        spoon.getLog().logError(builder.toString());
      }
    }

    return result;
  }


}
