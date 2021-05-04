package org.kettle.beam.pipeline.handler;

import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.transform.BeamFirestoreInputTransform;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.metastore.BeamJobConfig;
import org.kettle.beam.steps.firestore.BeamFirestoreInputMeta;
import org.kettle.beam.util.AuthUtil;
import org.kettle.beam.util.BeamConst;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamFirestoreInputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

    public BeamFirestoreInputStepHandler(BeamJobConfig beamJobConfig, IMetaStore metaStore, TransMeta transMeta, List<String> stepPluginClasses, List<String> xpPluginClasses) {
        super(beamJobConfig, true, false, metaStore, transMeta, stepPluginClasses, xpPluginClasses);
    }

    @Override
    public void handleStep(LogChannelInterface log, StepMeta beamOutputStepMeta, Map<String, PCollection<KettleRow>> stepCollectionMap,
                           Pipeline pipeline, RowMetaInterface rowMeta, List<StepMeta> previousSteps,
                           PCollection<KettleRow> input) throws KettleException {

        BeamFirestoreInputMeta meta = (BeamFirestoreInputMeta) beamOutputStepMeta.getStepMetaInterface();

        // Output rows (fields selection)
        //
        RowMetaInterface outputRowMeta = new RowMeta();
        meta.getFields( outputRowMeta, beamOutputStepMeta.getName(), null, null, transMeta, null, null );

        try {
            BeamFirestoreInputTransform transform = new BeamFirestoreInputTransform(
                    beamOutputStepMeta.getName(),
                    transMeta.environmentSubstitute(getProjectId()),
                    transMeta.environmentSubstitute(meta.getKind()),
                    transMeta.environmentSubstitute(meta.getGqlQuery()),
                    JsonRowMeta.toJson(outputRowMeta),
                    stepPluginClasses,
                    xpPluginClasses
            );
            PCollection<KettleRow> afterInput = pipeline.apply( transform );
            stepCollectionMap.put( beamOutputStepMeta.getName(), afterInput );
            log.logBasic( "Handled step (FIRESTORE INPUT) : " + beamOutputStepMeta.getName() );
        } catch (Exception e) {
            log.logError(e.getMessage());
        }
    }

    public String getProjectId() throws Exception {
        String configPath = System.getenv(BeamConst.GOOGLE_CREDENTIALS_ENVIRONMENT_VARIABLE);
        ServiceAccountCredentials serviceAccount = (ServiceAccountCredentials) AuthUtil.getCredentials(configPath);

        return serviceAccount.getProjectId();
    }
}
