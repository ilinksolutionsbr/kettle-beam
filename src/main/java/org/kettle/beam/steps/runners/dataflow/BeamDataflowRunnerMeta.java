package org.kettle.beam.steps.runners.dataflow;

import org.kettle.beam.steps.database.BeamDatabaseConnectorData;
import org.kettle.beam.steps.runners.RunnerParameter;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Classe respnsável por gerencier os meta dados spoon para o componente
 * Dataflow Runner.
 *
 */
@Step(
        id = BeamConst.STRING_BEAM_DATAFLOW_RUNNER_PLUGIN_ID,
        name = "Beam Dataflow Runner",
        description = "Dataflow Runner",
        image = "jobentry-dataflow-runner.svg",
        categoryDescription = "Big Data"
)
public class BeamDataflowRunnerMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String NAME = "name";
    public static final String USER_AGENT = "user_agent";
    public static final String TEMP_LOCATION = "temp_location";
    public static final String PLUGINS_TO_STAGE = "plugins_to_stage";
    public static final String STEP_PLUGIN_CLASSES = "step_plugin_classes";
    public static final String XP_PLUGIN_CLASSES = "xp_plugin_classes";
    public static final String STREAMING_KETTLE_STEPS_FLUSH_INTERVAL = "streaming_kettle_steps_flush_interval";
    public static final String FAT_JAR = "fat_jar";

    public static final String PARAMETERS = "parameters";
    public static final String PARAMETER = "parameter";

    public static final String GCP_PROJECT_ID = "gcp_project_id";
    public static final String GCP_APP_NAME = "gcp_app_name";
    public static final String GCP_STAGING_LOCATION = "gcp_staging_location";
    public static final String GCP_TEMPLATE_LOCATION = "gcp_template_location";
    public static final String GCP_NETWORK = "gcp_network";
    public static final String GCP_SUB_NETWORK = "gcp_sub_network";
    public static final String GCP_INITIAL_NUMBER_OF_WORKERS = "gcp_initial_number_of_workers";
    public static final String GCP_MAXIMUM_NUMBER_OF_WORKERS = "gcp_maximum_number_of_workers";
    public static final String GCP_AUTO_SCALING_ALGORITHM = "gcp_auto_scaling_algorithm";
    public static final String GCP_WORKER_MACHINE_TYPE = "gcp_worker_machine_type";
    public static final String GCP_WORKER_DISK_TYPE = "gcp_worker_disk_type";
    public static final String GCP_DISK_SIZE_GB = "gcp_disk_size_gb";
    public static final String GCP_REGION = "gcp_region";
    public static final String GCP_ZONE = "gcp_zone";
    public static final String GCP_STREAMING = "gcp_streaming";

    // General Fields
    private String name;
    private String userAgent;
    private String tempLocation;
    private String pluginsToStage;
    private String stepPluginClasses;
    private String xpPluginClasses;
    private List<RunnerParameter> parameters;
    private String streamingKettleStepsFlushInterval;
    private String fatJar;

    //
    // Dataflow specific options
    //

    private String gcpProjectId;
    private String gcpAppName;
    private String gcpStagingLocation;
    private String gcpTemplateLocation;
    private String gcpNetwork;
    private String gcpSubNetwork;
    private String gcpInitialNumberOfWorkers;
    private String gcpMaximumNumberOfWokers;
    private String gcpAutoScalingAlgorithm;
    private String gcpWorkerMachineType;
    private String gcpWorkerDiskType;
    private String gcpDiskSizeGb;
    private String gcpRegion;
    private String gcpZone;
    private boolean gcpStreaming;

    public BeamDataflowRunnerMeta() {
        super();
        parameters = new ArrayList<>();
    }

    @Override
    public void setDefault() {
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        StepInterface step = null;
        if (BeamConst.STRING_BEAM_DATAFLOW_RUNNER_PLUGIN_ID.equalsIgnoreCase(stepMeta.getStepID())) {
            step = new BeamDataflowRunner(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        }
        return step;
    }

    @Override
    public StepDataInterface getStepData() {
        return new BeamDatabaseConnectorData();
    }

    @Override
    public String getXML() throws KettleException {
        StringBuffer xml = new StringBuffer();

        xml.append( XMLHandler.addTagValue( NAME, name ) );
        xml.append( XMLHandler.addTagValue( USER_AGENT, userAgent ) );
        xml.append( XMLHandler.addTagValue( TEMP_LOCATION, tempLocation ) );
        xml.append( XMLHandler.addTagValue( PLUGINS_TO_STAGE, pluginsToStage ) );
        xml.append( XMLHandler.addTagValue( STEP_PLUGIN_CLASSES, stepPluginClasses ) );
        xml.append( XMLHandler.addTagValue( XP_PLUGIN_CLASSES, xpPluginClasses ) );
        xml.append( XMLHandler.addTagValue( STREAMING_KETTLE_STEPS_FLUSH_INTERVAL, streamingKettleStepsFlushInterval ) );
        xml.append( XMLHandler.addTagValue( FAT_JAR, fatJar ) );

        xml.append( XMLHandler.addTagValue( GCP_PROJECT_ID, gcpProjectId ) );
        xml.append( XMLHandler.addTagValue( GCP_APP_NAME, gcpAppName ) );
        xml.append( XMLHandler.addTagValue( GCP_STAGING_LOCATION, gcpStagingLocation ) );
        xml.append( XMLHandler.addTagValue( GCP_TEMPLATE_LOCATION, gcpTemplateLocation ) );
        xml.append( XMLHandler.addTagValue( GCP_NETWORK, gcpNetwork ) );
        xml.append( XMLHandler.addTagValue( GCP_SUB_NETWORK, gcpSubNetwork ) );
        xml.append( XMLHandler.addTagValue( GCP_INITIAL_NUMBER_OF_WORKERS, gcpInitialNumberOfWorkers ) );
        xml.append( XMLHandler.addTagValue( GCP_MAXIMUM_NUMBER_OF_WORKERS, gcpMaximumNumberOfWokers ) );
        xml.append( XMLHandler.addTagValue( GCP_AUTO_SCALING_ALGORITHM, gcpAutoScalingAlgorithm ) );
        xml.append( XMLHandler.addTagValue( GCP_WORKER_MACHINE_TYPE, gcpWorkerMachineType ) );
        xml.append( XMLHandler.addTagValue( GCP_WORKER_DISK_TYPE, gcpWorkerDiskType ) );
        xml.append( XMLHandler.addTagValue( GCP_DISK_SIZE_GB, gcpDiskSizeGb ) );
        xml.append( XMLHandler.addTagValue( GCP_REGION, gcpRegion ) );
        xml.append( XMLHandler.addTagValue( GCP_ZONE, gcpZone ) );
        xml.append( XMLHandler.addTagValue( GCP_STREAMING, gcpStreaming ) );

        xml.append( XMLHandler.openTag( PARAMETERS ) );
        for ( RunnerParameter parameter : this.getParameters() ) {
            xml.append( XMLHandler.openTag( PARAMETER ) );
            xml.append( XMLHandler.addTagValue( "variable", parameter.getVariable() ) );
            xml.append( XMLHandler.addTagValue( "value", parameter.getValue() ) );
            xml.append( XMLHandler.closeTag( PARAMETER ) );
        }
        xml.append( XMLHandler.closeTag( PARAMETERS ) );

        return xml.toString();
    }

    @Override
    public void loadXML(Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
        name = XMLHandler.getTagValue( stepNode, NAME );
        userAgent = XMLHandler.getTagValue( stepNode, USER_AGENT );
        tempLocation = XMLHandler.getTagValue( stepNode, TEMP_LOCATION );
        pluginsToStage = XMLHandler.getTagValue( stepNode, PLUGINS_TO_STAGE );
        stepPluginClasses = XMLHandler.getTagValue( stepNode, STEP_PLUGIN_CLASSES );
        xpPluginClasses = XMLHandler.getTagValue( stepNode, XP_PLUGIN_CLASSES );
        streamingKettleStepsFlushInterval = XMLHandler.getTagValue( stepNode, STREAMING_KETTLE_STEPS_FLUSH_INTERVAL );
        fatJar = XMLHandler.getTagValue( stepNode, FAT_JAR );

        gcpProjectId = XMLHandler.getTagValue( stepNode, GCP_PROJECT_ID );
        gcpAppName = XMLHandler.getTagValue( stepNode, GCP_APP_NAME );
        gcpStagingLocation = XMLHandler.getTagValue( stepNode, GCP_STAGING_LOCATION );
        gcpTemplateLocation = XMLHandler.getTagValue( stepNode, GCP_TEMPLATE_LOCATION );
        gcpNetwork = XMLHandler.getTagValue( stepNode, GCP_NETWORK );
        gcpSubNetwork = XMLHandler.getTagValue( stepNode, GCP_SUB_NETWORK );
        gcpInitialNumberOfWorkers = XMLHandler.getTagValue( stepNode, GCP_INITIAL_NUMBER_OF_WORKERS );
        gcpMaximumNumberOfWokers = XMLHandler.getTagValue( stepNode, GCP_MAXIMUM_NUMBER_OF_WORKERS );
        gcpAutoScalingAlgorithm = XMLHandler.getTagValue( stepNode, GCP_AUTO_SCALING_ALGORITHM );
        gcpWorkerMachineType = XMLHandler.getTagValue( stepNode, GCP_WORKER_MACHINE_TYPE );
        gcpWorkerDiskType = XMLHandler.getTagValue( stepNode, GCP_WORKER_DISK_TYPE );
        gcpDiskSizeGb = XMLHandler.getTagValue( stepNode, GCP_DISK_SIZE_GB );
        gcpRegion = XMLHandler.getTagValue( stepNode, GCP_REGION );
        gcpZone = XMLHandler.getTagValue( stepNode, GCP_ZONE );
        gcpStreaming = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, GCP_STREAMING ) );

        Node parametersNode = XMLHandler.getSubNode( stepNode, PARAMETERS );
        List<Node> parameterNodes = XMLHandler.getNodes( parametersNode, PARAMETER );
        String variable;
        String value;
        for(Node parameterNode: parameterNodes) {
            variable = XMLHandler.getTagValue( parameterNode, "variable" );
            value = XMLHandler.getTagValue( parameterNode, "value" );
            this.getParameters().add(new RunnerParameter(variable, value));
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getTempLocation() {
        return tempLocation;
    }

    public void setTempLocation(String tempLocation) {
        this.tempLocation = tempLocation;
    }

    public String getPluginsToStage() {
        return pluginsToStage;
    }

    public void setPluginsToStage(String pluginsToStage) {
        this.pluginsToStage = pluginsToStage;
    }

    public String getStepPluginClasses() {
        return stepPluginClasses;
    }

    public void setStepPluginClasses(String stepPluginClasses) {
        this.stepPluginClasses = stepPluginClasses;
    }

    public String getXpPluginClasses() {
        return xpPluginClasses;
    }

    public void setXpPluginClasses(String xpPluginClasses) {
        this.xpPluginClasses = xpPluginClasses;
    }

    public List<RunnerParameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<RunnerParameter> parameters) {
        this.parameters = parameters;
    }

    public String getStreamingKettleStepsFlushInterval() {
        return streamingKettleStepsFlushInterval;
    }

    public void setStreamingKettleStepsFlushInterval(String streamingKettleStepsFlushInterval) {
        this.streamingKettleStepsFlushInterval = streamingKettleStepsFlushInterval;
    }

    public String getFatJar() {
        return fatJar;
    }

    public void setFatJar(String fatJar) {
        this.fatJar = fatJar;
    }

    public String getGcpProjectId() {
        return gcpProjectId;
    }

    public void setGcpProjectId(String gcpProjectId) {
        this.gcpProjectId = gcpProjectId;
    }

    public String getGcpAppName() {
        return gcpAppName;
    }

    public void setGcpAppName(String gcpAppName) {
        this.gcpAppName = gcpAppName;
    }

    public String getGcpStagingLocation() {
        return gcpStagingLocation;
    }

    public void setGcpStagingLocation(String gcpStagingLocation) {
        this.gcpStagingLocation = gcpStagingLocation;
    }

    public String getGcpTemplateLocation() {
        return gcpTemplateLocation;
    }

    public void setGcpTemplateLocation(String gcpTemplateLocation) {
        this.gcpTemplateLocation = gcpTemplateLocation;
    }

    public String getGcpNetwork() {
        return gcpNetwork;
    }

    public void setGcpNetwork(String gcpNetwork) {
        this.gcpNetwork = gcpNetwork;
    }

    public String getGcpSubNetwork() {
        return gcpSubNetwork;
    }

    public void setGcpSubNetwork(String gcpSubNetwork) {
        this.gcpSubNetwork = gcpSubNetwork;
    }

    public String getGcpInitialNumberOfWorkers() {
        return gcpInitialNumberOfWorkers;
    }

    public void setGcpInitialNumberOfWorkers(String gcpInitialNumberOfWorkers) {
        this.gcpInitialNumberOfWorkers = gcpInitialNumberOfWorkers;
    }

    public String getGcpMaximumNumberOfWokers() {
        return gcpMaximumNumberOfWokers;
    }

    public void setGcpMaximumNumberOfWokers(String gcpMaximumNumberOfWokers) {
        this.gcpMaximumNumberOfWokers = gcpMaximumNumberOfWokers;
    }

    public String getGcpAutoScalingAlgorithm() {
        return gcpAutoScalingAlgorithm;
    }

    public void setGcpAutoScalingAlgorithm(String gcpAutoScalingAlgorithm) {
        this.gcpAutoScalingAlgorithm = gcpAutoScalingAlgorithm;
    }

    public String getGcpWorkerMachineType() {
        return gcpWorkerMachineType;
    }

    public void setGcpWorkerMachineType(String gcpWorkerMachineType) {
        this.gcpWorkerMachineType = gcpWorkerMachineType;
    }

    public String getGcpWorkerDiskType() {
        return gcpWorkerDiskType;
    }

    public void setGcpWorkerDiskType(String gcpWorkerDiskType) {
        this.gcpWorkerDiskType = gcpWorkerDiskType;
    }

    public String getGcpDiskSizeGb() {
        return gcpDiskSizeGb;
    }

    public void setGcpDiskSizeGb(String gcpDiskSizeGb) {
        this.gcpDiskSizeGb = gcpDiskSizeGb;
    }

    public String getGcpRegion() {
        return gcpRegion;
    }

    public void setGcpRegion(String gcpRegion) {
        this.gcpRegion = gcpRegion;
    }

    public String getGcpZone() {
        return gcpZone;
    }

    public void setGcpZone(String gcpZone) {
        this.gcpZone = gcpZone;
    }

    public boolean isGcpStreaming() {
        return gcpStreaming;
    }

    public void setGcpStreaming(boolean gcpStreaming) {
        this.gcpStreaming = gcpStreaming;
    }
}
