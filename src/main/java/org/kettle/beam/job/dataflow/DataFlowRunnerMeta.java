package org.kettle.beam.job.dataflow;

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
        name = "GCP DataFlow Runner",
        description = "Google Cloud DataFlow Runner",
        image = "jobentry-dataflow-runner.svg",
        categoryDescription = "Big Data"
)
public class DataFlowRunnerMeta extends JobEntryBase implements Cloneable, JobEntryInterface {

    /**
     *  The PKG member is used when looking up internationalized strings.
     *  The properties file with localized keys is expected to reside in
     *  {the package of the class specified}/messages/messages_{locale}.properties
     */
    private static Class<?> PKG = DataFlowRunnerMeta.class; // for i18n purposes $NON-NLS-1$

    public static final String JOB_NAME = "job_name";

    private String jobName;

    private DataFlowRunner jobEntry;


    /**
     * The JobEntry constructor executes super() and initializes its fields
     * with sensible defaults for new instances of the job entry.
     *
     * @param name the name of the new job entry
     */
    public DataFlowRunnerMeta( String name ) {
        super( name, "" );
    }

    /**
     * No-Arguments constructor for convenience purposes.
     */
    public DataFlowRunnerMeta() {
        this( "" );
    }

    /**
     * Let PDI know the class name to use for the dialog.
     * @return the class name to use for the dialog for this job entry
     */
    public String getDialogClassName() {
        return DataFlowRunnerDialog.class.getName();
    }

    public DataFlowRunner getJobEntry(){
        if(jobEntry == null){jobEntry = new DataFlowRunner(this);}
        return jobEntry;
    }

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
        DataFlowRunnerMeta entry = (DataFlowRunnerMeta) super.clone();
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
        xml.append( XMLHandler.addTagValue( JOB_NAME, jobName ) );
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
        jobName = XMLHandler.getTagValue( entryNode, JOB_NAME );
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

    /**
     * Returns true if the job entry offers a genuine true/false result upon execution,
     * and thus supports separate "On TRUE" and "On FALSE" outgoing hops.
     */
    public boolean evaluates() {
        return true;
    }

    /**
     * Returns true if the job entry supports unconditional outgoing hops.
     */
    public boolean isUnconditional() {
        return false;
    }



    public String getJobName() {
        return jobName;
    }
    public void setJobName(String jobName ) {
        this.jobName = jobName;
    }


}
