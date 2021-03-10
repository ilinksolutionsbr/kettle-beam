package org.kettle.beam.job.gcp;

import org.kettle.beam.util.BeamConst;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@JobEntry(
        id = BeamConst.STRING_JOB_ENTRY_DATAFLOW_RUNNER_PLUGIN_ID,
        name = "DataFlow Runner",
        description = "Google Cloud DataFlow Runner",
        image = "jobentry-dataflow-runner.svg",
        categoryDescription = "Google Cloud Workflow"
)
public class DataflowRunnerMeta extends JobEntryBase implements Cloneable, JobEntryInterface {

    //region Attributes

    /**
     *  The PKG member is used when looking up internationalized strings.
     *  The properties file with localized keys is expected to reside in
     *  {the package of the class specified}/messages/messages_{locale}.properties
     */
    private static Class<?> PKG = DataflowRunnerMeta.class;
    private DataflowRunner jobEntry;

    public static final String PROJECT_ID = "project_id";
    public static final String REGION = "region";
    public static final String ZONE = "zone";
    public static final String JOB_NAME = "job_name";
    public static final String TEMPLATE_LOCATION = "template_location";
    public static final String TEMP_LOCATION = "temp_location";
    public static final String WAIT_SERVICE = "WAIT_SERVICE";

    private String projectId;
    private String region;
    private String zone;
    private String jobName;
    private String templateLocation;
    private String tempLocation;
    private String waitService;


    //endregion

    //region Getters Setters

    /**
     * Let PDI know the class name to use for the dialog.
     * @return the class name to use for the dialog for this job entry
     */
    public String getDialogClassName() {
        return DataflowRunnerDialog.class.getName();
    }

    public DataflowRunner getJobEntry(){
        if(jobEntry == null){jobEntry = new DataflowRunner(this);}
        return jobEntry;
    }

    /**
     * Returns true if the job entry offers a genuine true/false result upon execution,
     * and thus supports separate "On TRUE" and "On FALSE" outgoing hops.
     */
    public boolean evaluates() {
        return false;
    }

    /**
     * Returns true if the job entry supports unconditional outgoing hops.
     */
    public boolean isUnconditional() {
        return false;
    }



    public String getProjectId() {return projectId;}
    public void setProjectId(String projectId ) {
        this.projectId = projectId;
    }

    public String getRegion() {return region;}
    public void setRegion(String region ) {
        this.region = region;
    }

    public String getZone() {return zone;}
    public void setZone(String zone ) {
        this.zone = zone;
    }

    public String getJobName() {return jobName;}
    public void setJobName(String jobName ) {
        this.jobName = jobName;
    }

    public String getTemplateLocation() {return templateLocation;}
    public void setTemplateLocation(String templateLocation ) {
        this.templateLocation = templateLocation;
    }

    public String getTempLocation() {return tempLocation;}
    public void setTempLocation(String tempLocation ) {
        this.tempLocation = tempLocation;
    }

    public String getWaitService() {return waitService;}
    public void setWaitService(String waitService ) {
        this.waitService = waitService;
    }


    //endregion

    //region Constructors

    /**
     * The JobEntry constructor executes super() and initializes its fields
     * with sensible defaults for new instances of the job entry.
     *
     * @param name the name of the new job entry
     */
    public DataflowRunnerMeta(String name ) {
        super( name, "" );
    }

    /**
     * No-Arguments constructor for convenience purposes.
     */
    public DataflowRunnerMeta() {
        this( "" );
    }

    //endregion

    //region Methods

    /**
     * This method is used when a job entry is duplicated in Spoon. It needs to return a deep copy of this
     * job entry object. Be sure to create proper deep copies if the job entry configuration is stored in
     * modifiable objects.
     *
     * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on creating
     * a deep copy of an object.
     *
     * @return a deep copy of this
     */
    public Object clone() {
        DataflowRunnerMeta entry = (DataflowRunnerMeta) super.clone();
        return entry;
    }

    /**
     * This method is called by Spoon when a job entry needs to serialize its configuration to XML. The expected
     * return value is an XML fragment consisting of one or more XML tags.
     *
     * Please use org.pentaho.di.core.xml.XMLHandler to conveniently generate the XML.
     *
     * Note: the returned string must include the output of super.getXML() as well
     * @return a string containing the XML serialization of this job entry
     */
    @Override
    public String getXML() {
        StringBuffer xml = new StringBuffer();
        xml.append( super.getXML() );
        xml.append( XMLHandler.addTagValue( PROJECT_ID, projectId ) );
        xml.append( XMLHandler.addTagValue( REGION, region ) );
        xml.append( XMLHandler.addTagValue( ZONE, zone ) );
        xml.append( XMLHandler.addTagValue( JOB_NAME, jobName ) );
        xml.append( XMLHandler.addTagValue( TEMPLATE_LOCATION, templateLocation ) );
        xml.append( XMLHandler.addTagValue( TEMP_LOCATION, tempLocation ) );
        xml.append( XMLHandler.addTagValue( WAIT_SERVICE, waitService ) );
        return xml.toString();
    }

    /**
     * This method is called by PDI when a job entry needs to load its configuration from XML.
     *
     * Please use org.pentaho.di.core.xml.XMLHandler to conveniently read from the
     * XML node passed in.
     *
     * Note: the implementation must call super.loadXML() to ensure correct behavior
     *
     * @param entryNode    the XML node containing the configuration
     * @param databases    the databases available in the job
     * @param slaveServers the slave servers available in the job
     * @param repository   the repository connected to, if any
     * @param metaStore    the metastore to optionally read from
     */
    @Override
    public void loadXML(Node entryNode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository repository, IMetaStore metaStore ) throws KettleXMLException {
        super.loadXML( entryNode, databases, slaveServers );
        projectId = XMLHandler.getTagValue( entryNode, PROJECT_ID );
        region = XMLHandler.getTagValue( entryNode, REGION );
        zone = XMLHandler.getTagValue( entryNode, ZONE );
        jobName = XMLHandler.getTagValue( entryNode, JOB_NAME );
        templateLocation = XMLHandler.getTagValue( entryNode, TEMPLATE_LOCATION );
        tempLocation = XMLHandler.getTagValue( entryNode, TEMP_LOCATION );
        waitService = XMLHandler.getTagValue( entryNode, WAIT_SERVICE );
    }

    /**
     * This method is called when it is the job entry's turn to run during the execution of a job.
     * It should return the passed in Result object, which has been updated to reflect the outcome
     * of the job entry. The execute() method should call setResult(), setNrErrors() and modify the
     * rows or files attached to the result object if required.
     *
     * @param prevResult The result of the previous execution
     * @return The Result of the execution.
     */
    public Result execute(Result prevResult, int nr ) {
        return this.getJobEntry().execute(prevResult, nr);
    }


    //endregion

}
