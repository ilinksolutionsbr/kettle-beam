package org.kettle.beam.pipeline.main;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.metastore.SerializableMetaStore;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.pipeline.KettleBeamPipelineExecutor;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainBeam {

  public static final int mainMethod( final String[] args, final String environment ) {

    try {
      System.out.println( "Starting clustered transformation execution on environment: '"+environment+"'" );

      System.out.println( "Transformation ktr / args[0] : " + args[ 0 ] );
      System.out.println( "MetaStore JSON     / args[1] : " + args[ 1 ] );
      System.out.println( "Beam Job Config    / args[2] : " + args[ 2 ] );

      // Read the transformation XML and MetaStore from Hadoop FS
      //
      Configuration hadoopConfiguration = new Configuration();
      String transMetaXml = readFileIntoString( args[ 0 ], hadoopConfiguration, "UTF-8" );
      String metaStoreJson = readFileIntoString( args[ 1 ], hadoopConfiguration, "UTF-8" );

      // Third argument: the beam job config
      //
      String jobConfigName = args[ 2 ];

      // Inflate the metaStore...
      //
      IMetaStore metaStore = new SerializableMetaStore( metaStoreJson );

      System.out.println( ">>>>>> Loading Kettle Beam Job Config '" + jobConfigName + "'" );
      MetaStoreFactory<BeamJobConfig> configFactory = new MetaStoreFactory<>( BeamJobConfig.class, metaStore, PentahoDefaults.NAMESPACE );
      BeamJobConfig jobConfig = configFactory.loadElement( jobConfigName );

      List<String> stepPluginsList = new ArrayList<>( Arrays.asList( Const.NVL(jobConfig.getStepPluginClasses(), "").split( "," ) ) );
      List<String> xpPluginsList = new ArrayList<>( Arrays.asList( Const.NVL(jobConfig.getXpPluginClasses(), "").split( "," ) ) );

      stepPluginsList.removeIf(i -> i.trim().isEmpty());
      xpPluginsList.removeIf(i -> i.trim().isEmpty());

      System.out.println(">>>>>> Steps loaded : ");
      for(String step: stepPluginsList) {
        System.out.println(step);
      }

      System.out.println(">>>>>> Plugins loaded : ");
      for(String plugin: xpPluginsList) {
        System.out.println(plugin);
      }

      System.out.println( ">>>>>> Initializing Kettle runtime (" + stepPluginsList.size() + " step classes, " + xpPluginsList.size() + " XP classes)" );

      BeamKettle.init( stepPluginsList, xpPluginsList );

      System.out.println( ">>>>>> Loading transformation metadata" );
      TransMeta transMeta = new TransMeta( XMLHandler.loadXMLString( transMetaXml, TransMeta.XML_TAG ), null );
      transMeta.setMetaStore( metaStore );


      String hadoopConfDir = System.getenv( "HADOOP_CONF_DIR" );
      System.out.println( ">>>>>> HADOOP_CONF_DIR='" + hadoopConfDir + "'" );

      System.out.println( ">>>>>> Building Apache Beam Kettle Pipeline..." );
      PluginRegistry registry = PluginRegistry.getInstance();
      PluginInterface beamInputPlugin = registry.getPlugin( StepPluginType.class, BeamConst.STRING_BEAM_INPUT_PLUGIN_ID );
      if ( beamInputPlugin != null ) {
        System.out.println( ">>>>>> Found Beam Input step plugin class loader" );
      } else {
        throw new KettleException( "Unable to find Beam Input step plugin, bailing out!" );
      }
      ClassLoader pluginClassLoader = PluginRegistry.getInstance().getClassLoader( beamInputPlugin );
      if ( pluginClassLoader != null ) {
        System.out.println( ">>>>>> Found Beam Input step plugin class loader" );
      } else {
        System.out.println( ">>>>>> NOT found Beam Input step plugin class loader, using system classloader" );
        pluginClassLoader = ClassLoader.getSystemClassLoader();
      }
      KettleBeamPipelineExecutor executor = new KettleBeamPipelineExecutor( LogChannel.GENERAL, transMeta, jobConfig, metaStore, pluginClassLoader, stepPluginsList, xpPluginsList );

      System.out.println( ">>>>>> Pipeline executing starting..." );
      executor.setLoggingMetrics( true );
      executor.execute( true );
      System.out.println( ">>>>>> Execution finished..." );
      return 0;
    } catch ( Exception e ) {
      System.err.println( "Error running Beam pipeline on '"+environment+"': " + e.getMessage() );
      e.printStackTrace();
      return 1;
    }

  }

  private static String readFileIntoString( String filename, Configuration hadoopConfiguration, String encoding ) throws IOException {
    Path path = new Path( filename );
    FileSystem fileSystem = FileSystem.get( path.toUri(), hadoopConfiguration );
    FSDataInputStream inputStream = fileSystem.open( path );
    String fileContent = IOUtils.toString( inputStream, encoding );
    return fileContent;
  }
}
