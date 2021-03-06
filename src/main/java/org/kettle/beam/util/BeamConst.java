package org.kettle.beam.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

public class BeamConst {

  public static final String STRING_JOB_ENTRY_DATAFLOW_RUNNER_PLUGIN_ID = "DataflowRunnerJobEntry";
  public static final String STRING_JOB_ENTRY_WORKFLOW_FINISHER_PLUGIN_ID = "WorkflowFinisherJobEntry";

  public static final String STRING_BEAM_INPUT_PLUGIN_ID = "BeamInput";
  public static final String STRING_BEAM_OUTPUT_PLUGIN_ID = "BeamOutput";
  public static final String STRING_BEAM_SUBSCRIBE_PLUGIN_ID = "BeamSubscribe";
  public static final String STRING_BEAM_PUBLISH_PLUGIN_ID = "BeamPublish";
  public static final String STRING_MEMORY_GROUP_BY_PLUGIN_ID = "MemoryGroupBy";
  public static final String STRING_MERGE_JOIN_PLUGIN_ID = "MergeJoin";
  public static final String STRING_BEAM_WINDOW_PLUGIN_ID = "BeamWindow";
  public static final String STRING_BEAM_TIMESTAMP_PLUGIN_ID = "BeamTimestamp";
  public static final String STRING_BEAM_BIGQUERY_INPUT_PLUGIN_ID = "BeamBQInput";
  public static final String STRING_BEAM_BIGQUERY_OUTPUT_PLUGIN_ID = "BeamBQOutput";
  public static final String STRING_BEAM_KAFKA_CONSUME_PLUGIN_ID = "BeamKafkaConsume";
  public static final String STRING_BEAM_KAFKA_PRODUCE_PLUGIN_ID = "BeamKafkaProduce";
  public static final String STRING_BEAM_FIRESTORE_INPUT_PLUGIN_ID = "BeamFirestoreInput";
  public static final String STRING_BEAM_FIRESTORE_OUTPUT_PLUGIN_ID = "BeamFirestoreOutput";
  public static final String STRING_BEAM_DATABASE_PLUGIN_ID = "BeamDatabaseConnector";
  public static final String STRING_BEAM_JSON_PARSER_PLUGIN_ID = "BeamJSONParser";
  public static final String STRING_BEAM_PROCEDURE_EXECUTOR_PLUGIN_ID = "BeamProcedureExecutor";
  public static final String STRING_BEAM_DATAFLOW_RUNNER_PLUGIN_ID = "BeamDataflowRunner";

  public static final String STRING_KETTLE_BEAM = "Kettle Beam";

  public static final String STRING_STEP_FLAG_BATCH = "Batch";
  public static final String STRING_STEP_FLAG_SINGLE_THREADED = "SingleThreaded";

  public static final String FILE_GET_FIELDS_TYPE_STRING = "String";
  public static final String FILE_GET_FIELDS_TYPE_BOOLEAN = "Boolean";
  public static final String FILE_GET_FIELDS_TYPE_BINARY = "Binary";
  public static final String FILE_GET_FIELDS_TYPE_INTEGER = "Integer";
  public static final String FILE_GET_FIELDS_TYPE_NUMBER = "Number";
  public static final String FILE_GET_FIELDS_TYPE_TIMESTAMP = "Timestamp";
  public static final String FILE_GET_FIELDS_TYPE_DATE = "Date";

  public static final String GOOGLE_CREDENTIALS_ENVIRONMENT_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";

  private static List<String[]> gcpWorkerCodeDescriptions = Arrays.asList(
    new String[] { "n1-standard-1", "Standard machine type with 1 vCPU and 3.75 GB of memory." },
    new String[] { "n1-standard-2", "Standard machine type with 2 vCPUs and 7.5 GB of memory." },
    new String[] { "n1-standard-4", "Standard machine type with 4 vCPUs and 15 GB of memory." },
    new String[] { "n1-standard-8", "Standard machine type with 8 vCPUs and 30 GB of memory." },
    new String[] { "n1-standard-16", "Standard machine type with 16 vCPUs and 60 GB of memory." },
    new String[] { "n1-standard-32", "Standard machine type with 32 vCPUs and 120 GB of memory." },
    new String[] { "n1-standard-64", "Standard machine type with 64 vCPUs and 240 GB of memory." },
    new String[] { "n1-standard-96", "Standard machine type with 96 vCPUs and 360 GB of memory." },
    new String[] { "n1-highmem-2", "High memory machine type with 2 vCPUs and 13 GB of memory." },
    new String[] { "n1-highmem-4", "High memory machine type with 4 vCPUs, and 26 GB of memory." },
    new String[] { "n1-highmem-8", "High memory machine type with 8 vCPUs and 52 GB of memory." },
    new String[] { "n1-highmem-16", "High memory machine type with 16 vCPUs and 104 GB of memory." },
    new String[] { "n1-highmem-32", "High memory machine type with 32 vCPUs and 208 GB of memory." },
    new String[] { "n1-highmem-64", "High memory machine type with 64 vCPUs and 416 GB of memory." },
    new String[] { "n1-highmem-96", "High memory machine type with 96 vCPUs and 624 GB of memory." },
    new String[] { "n1-highcpu-2", "High-CPU machine type with 2 vCPUs and 1.80 GB of memory." },
    new String[] { "n1-highcpu-4", "High-CPU machine type with 4 vCPUs and 3.60 GB of memory." },
    new String[] { "n1-highcpu-8", "High-CPU machine type with 8 vCPUs and 7.20 GB of memory." },
    new String[] { "n1-highcpu-16", "High-CPU machine type with 16 vCPUs and 14.4 GB of memory." },
    new String[] { "n1-highcpu-32", "High-CPU machine type with 32 vCPUs and 28.8 GB of memory." },
    new String[] { "n1-highcpu-64", "High-CPU machine type with 64 vCPUs and 57.6 GB of memory." },
    new String[] { "n1-highcpu-96", "High-CPU machine type with 96 vCPUs and 86.4 GB of memory." },
    new String[] { "f1-micro", "Micro machine type with 0.2 vCPU, 0.60 GB of memory, backed by a shared physical core." },
    new String[] { "g1-small", "Shared-core machine type with 0.5 vCPU, 1.70 GB of memory, backed by a shared physical core." },
    new String[] { "n1-ultramem-40", "Memory-optimized machine type with 40 vCPUs and 961GB of memory." },
    new String[] { "n1-ultramem-80", "Memory-optimized machine type with 80 vCPUs and 1.87 TB of memory." },
    new String[] { "n1-megamem-96", "Memory-optimized machine type with 96 vCPUs and 1.4 TB of memory." },
    new String[] { "n1-ultramem-160", "Memory-optimized machine type with 160 vCPUs and 3.75 TB of memory." }
  );

  private static List<String[]> gcpRegionCodeZonesDescriptions = Arrays.asList(
    new String[] { "asia-east1", "a, b, c", "Changhua County, Taiwan" },
    new String[] { "asia-east2", "a, b, c", "Hong Kong" },
    new String[] { "asia-northeast1", "a, b, c", "Tokyo, Japan" },
    new String[] { "asia-south1", "a, b, c", "Mumbai, India" },
    new String[] { "asia-southeast1", "a, b, c", "Jurong West, Singapore" },
    new String[] { "australia-southeast1", "a, b, c", "Sydney, Australia" },
    new String[] { "europe-north1", "a, b, c", "Hamina, Finland" },
    new String[] { "europe-west1", "b, c, d", "St. Ghislain, Belgium" },
    new String[] { "europe-west2", "a, b, c", "London, England, UK" },
    new String[] { "europe-west3", "a, b, c", "Frankfurt, Germany" },
    new String[] { "europe-west4", "a, b, c", "Eemshaven, Netherlands" },
    new String[] { "northamerica-northeast1", "a, b, c", "Montréal, Québec, Canada" },
    new String[] { "southamerica-east1", "a, b, c", "São Paulo, Brazil" },
    new String[] { "us-central1", "a, b, c, f", "Council Bluffs, Iowa, USA" },
    new String[] { "us-east1", "b, c, d", "Moncks Corner, South Carolina, USA" },
    new String[] { "us-east4", "a, b, c", "Ashburn, Northern Virginia, USA" },
    new String[] { "us-west1", "a, b, c", "The Dalles, Oregon, USA" },
    new String[] { "us-west2", "a, b, c", "Los Angeles, California, USA" }
  );

  public static final String[] getGcpWorkerMachineTypeCodes() {
    String[] codes = new String[gcpWorkerCodeDescriptions.size()];
    for (int i=0;i<codes.length;i++) {
      codes[i] = gcpWorkerCodeDescriptions.get(i)[0];
    }
    return codes;
  }

  public static final String[] getGcpWorkerMachineTypeDescriptions() {
    String[] descriptions = new String[gcpWorkerCodeDescriptions.size()];
    for (int i=0;i<descriptions.length;i++) {
      descriptions[i] = gcpWorkerCodeDescriptions.get(i)[1];
    }
    return descriptions;
  }

  public static final String[] getGcpRegionCodes() {
    String[] codes = new String[gcpRegionCodeZonesDescriptions.size()];
    for (int i=0;i<codes.length;i++) {
      codes[i] = gcpRegionCodeZonesDescriptions.get(i)[0];
    }
    return codes;
  }

  public static final String[] getGcpRegionDescriptions() {
    String[] descriptions = new String[gcpRegionCodeZonesDescriptions.size()];
    for (int i=0;i<descriptions.length;i++) {
      descriptions[i] = gcpRegionCodeZonesDescriptions.get(i)[2];
    }
    return descriptions;
  }

  public static final String[] getGcpRegionNames() {
    String[] descriptions = new String[gcpRegionCodeZonesDescriptions.size()];
    for (int i=0;i<descriptions.length;i++) {
      descriptions[i] = gcpRegionCodeZonesDescriptions.get(i)[0];
    }
    return descriptions;
  }

  public static ValueMetaInterface createValueMeta(String name, int type){
    switch (type){

      case Types.BINARY :
      case Types.BLOB :
      case Types.CLOB :
      case Types.LONGVARBINARY:
      case Types.NCLOB:
      case Types.VARBINARY: return new ValueMetaBinary(name);

      case Types.BIT :
      case Types.BOOLEAN : return new ValueMetaBoolean(name);

      case Types.CHAR : return new ValueMetaString(name);

      case Types.DATALINK: return new ValueMetaInternetAddress(name);

      case Types.DATE: return new ValueMetaDate(name);

      case Types.DECIMAL:
      case Types.DOUBLE:
      case Types.FLOAT:
      case Types.REAL: return new ValueMetaNumber(name);

      case Types.NUMERIC: return new ValueMetaBigNumber(name);

      case Types.BIGINT :
      case Types.INTEGER:
      case Types.SMALLINT:
      case Types.TINYINT: return new ValueMetaInteger(name);

      case Types.JAVA_OBJECT: return new ValueMetaSerializable(name);

      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NULL:
      case Types.NVARCHAR:
      case Types.OTHER:
      case Types.VARCHAR: return new ValueMetaString(name);

      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE: return new ValueMetaTimestamp(name);

      default: return new ValueMetaString(name);

    }
  }

  public static final List<String> findLibraryFilesToStage( String baseFolder, String pluginFolders, boolean includeParent, boolean includeBeam ) throws IOException {

    File base;
    if ( baseFolder == null ) {
      base = new File( "." );
    } else {
      base = new File( baseFolder );
    }

    Set<String> uniqueNames = new HashSet<>();
    List<String> libraries = new ArrayList<>();

    // A unique list of plugin folders
    //
    Set<String> pluginFoldersSet = new HashSet<>();
    if ( StringUtils.isNotEmpty( pluginFolders ) ) {
      String[] folders = pluginFolders.split( "," );
      for ( String folder : folders ) {
        pluginFoldersSet.add( folder );
      }
    }
    if (includeBeam) {
      // TODO: make this plugin folder configurable
      //
      pluginFoldersSet.add("kettle-beam");
    }

    // Now the selected plugins libs...
    //
    for ( String pluginFolder : pluginFoldersSet ) {
      File pluginsFolder = new File( base.toString() + "/plugins/" + pluginFolder );
      if (pluginsFolder.exists()) {
        Collection<File> pluginFiles = FileUtils.listFiles( pluginsFolder, new String[] { "jar" }, true );
        if ( pluginFiles != null ) {
          for ( File file : pluginFiles ) {
            String shortName = file.getName();
            if ( !uniqueNames.contains( shortName ) ) {
              uniqueNames.add( shortName );
              libraries.add( file.getCanonicalPath() );
            }
          }
        }
      } else {
        System.out.println("Warning: couldn't find plugins folder: "+pluginsFolder);
      }
    }

    // Add all the jar files in lib/ to the classpath.
    //
    if (includeParent) {

      File libFolder = new File( base.toString() + "/lib" );

      if (libFolder.exists()) {

        Collection<File> files = FileUtils.listFiles( libFolder, new String[] { "jar" }, true );
        if ( files != null ) {
          for ( File file : files ) {
            String shortName = file.getName();
            if ( !uniqueNames.contains( shortName ) ) {
              uniqueNames.add( shortName );
              libraries.add( file.getCanonicalPath() );
              // System.out.println( "Adding library : " + file.getAbsolutePath() );
            }
          }
        }
      } else {
        System.out.println("Warning: couldn't find kettle lib folder: "+libFolder);

      }
    }

    // Collections.sort(libraries);

    return libraries;
  }
}
