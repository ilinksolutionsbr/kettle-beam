package org.kettle.beam.util;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.commons.lang.StringUtils;

import java.io.FileInputStream;
import java.io.InputStream;

public class AuthUtil {

    public static GoogleCredentials getCredentials(String configPath) throws Exception {
        InputStream inputStream = null;
        GoogleCredentials credentials;
        if(StringUtils.isNotEmpty(configPath)){
            try {
                inputStream = new FileInputStream(configPath);
            }catch (Exception ex){
                inputStream = null;
            }
        }
        if(inputStream == null){
            throw new Exception(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Erro: Não foi possível carregar o arquivo de credenciais do Google Cloud, favor configurar a variável de ambiente '" + configPath + "' apontando para o arquivo JSON com as credenciais.");
        }
        try {
            credentials = GoogleCredentials.fromStream(inputStream);
        } catch (Exception e) {
            throw new Exception(BeamConst.STRING_BEAM_PUBLISH_PLUGIN_ID + " -> Erro ao carregar o arquivo de credenciais do Google Cloud.", e);
        }
        return credentials;
    }
}
