package org.kettle.beam.core.transform;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.Query;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.fn.FirestoreEntityToKettleRowFn;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.Strings;
import org.pentaho.di.core.row.RowMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BeamFirestoreInputTransform extends PTransform<PBegin, PCollection<KettleRow>> {

    // These non-transient privates get serialized to spread across nodes
    //
    private String stepname;
    private String projectId;
    private String kind;
    private String gqlQuery;
    private String rowMetaJson;
    private List<String> stepPluginClasses;
    private List<String> xpPluginClasses;

    // Log and count errors.
    private static final Logger LOG = LoggerFactory.getLogger( BeamFirestoreInputTransform.class );
    private static final Counter numErrors = Metrics.counter( "main", "BeamFirestoreOutputError" );

    public BeamFirestoreInputTransform() {
    }

    public BeamFirestoreInputTransform(String stepname, String projectId, String kind, String gqlQuery, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses) {
        this.stepname = stepname;
        this.projectId = projectId;
        this.kind = kind;
        this.gqlQuery = gqlQuery;
        this.rowMetaJson = rowMetaJson;
        this.stepPluginClasses = stepPluginClasses;
        this.xpPluginClasses = xpPluginClasses;
    }

    @Override public PCollection<KettleRow> expand( PBegin input ) {
        try {
            // Only initialize once on this node/vm
            //
            BeamKettle.init( stepPluginClasses, xpPluginClasses );

            // Inflate the metadata on the node where this is running...
            //
            RowMetaInterface rowMeta = JsonRowMeta.fromJson( rowMetaJson );

            // Datastore Reader
            //

            if(Strings.isNullOrEmpty(gqlQuery)) {
                gqlQuery = "select * from " + this.kind + " limit 10";
                LOG.info("No GQL query provided, using default query");
            }

            DatastoreV1.Read datastoreRead = DatastoreIO.v1()
                    .read()
                    .withLiteralGqlQuery(gqlQuery)
                    .withProjectId(projectId);

            FirestoreEntityToKettleRowFn firestoreEntityToKettleRowFn = new FirestoreEntityToKettleRowFn( stepname, this.projectId, this.kind, rowMetaJson, stepPluginClasses, xpPluginClasses );
            PCollection<KettleRow> output = input
                    .apply(datastoreRead)
                    .apply(ParDo.of(firestoreEntityToKettleRowFn));

            return output;

        } catch ( Exception e ) {
            numErrors.inc();
            LOG.error( "Error in Beam Firestore Input transform", e );
            throw new RuntimeException( "Error in Beam Firestore Input transform", e );
        }
    }
}
