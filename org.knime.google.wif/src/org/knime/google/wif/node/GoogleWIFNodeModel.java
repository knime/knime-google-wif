/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   May 16, 2024 (Bjoern Lohrmann, KNIME GmbH): created
 */
package org.knime.google.wif.node;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.lang3.ObjectUtils;
import org.knime.base.node.io.filehandling.webui.FileChooserPathAccessor;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformationPortObjectSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.CredentialCache;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.ReadPathAccessor;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.google.api.credential.GoogleCredential;
import org.knime.google.api.nodes.util.GoogleApiUtil;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.util.Data;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AwsCredentials;
import com.google.auth.oauth2.AwsSecurityCredentialsSupplier;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.IdentityPoolCredentialSource;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.PluggableAuthCredentialSource;
import com.google.auth.oauth2.PluggableAuthCredentials;

/**
 * The Databricks Workspace Connector node model.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
final class GoogleWIFNodeModel extends WebUINodeModel<GoogleWIFSettings> {

    private UUID m_credentialCacheKey;

    private final int m_fileIdx;
    private final int m_awsIdx;

    /**
     * @param portsConfig The node configuration.
     */
    GoogleWIFNodeModel(final PortsConfiguration portsConfig) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts(), GoogleWIFSettings.class);
        final PortType[] inputPorts = portsConfig.getInputPorts();
        if (inputPorts.length == 0) {
            m_awsIdx = -1;
            m_fileIdx = -1;
        } else if (inputPorts.length == 1) {
            if (AmazonConnectionInformationPortObject.TYPE.equals(inputPorts[0])) {
                m_fileIdx = -1;
                m_awsIdx = 0;
            } else {
                m_fileIdx = 0;
                m_awsIdx = -1;
            }
        } else {
            m_fileIdx = 0;
            m_awsIdx = 1;
        }
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs, final GoogleWIFSettings settings)
        throws InvalidSettingsException {
        if (m_awsIdx >= 0 && !(inSpecs[m_awsIdx] instanceof CloudConnectionInformationPortObjectSpec)) {
            throw new InvalidSettingsException(
                "Incompatible input connection. Connect the Amazon Authenticator output port.");
        }

        return new PortObjectSpec[]{new CredentialPortObjectSpec()};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final GoogleWIFSettings settings) throws Exception {
        final Optional<FSConnection> portObjectConnection;
        if (m_fileIdx >= 0) {
            final FileSystemPortObject portObject = (FileSystemPortObject)inObjects[m_fileIdx];
            portObjectConnection = portObject == null ? Optional.empty() : portObject.getFileSystemConnection();
        } else {
            portObjectConnection = Optional.empty();
        }

        final Optional<AwsSecurityCredentialsSupplier> awsSupplier;
        if (m_awsIdx >= 0) {
            final CloudConnectionInformationPortObjectSpec awsSpec =
                    (CloudConnectionInformationPortObjectSpec)((AmazonConnectionInformationPortObject)inObjects[m_awsIdx])
                    .getSpec();
            awsSupplier = Optional.of(new AwsSupplier(awsSpec.getConnectionInformation()));
        } else {
            awsSupplier = Optional.empty();
        }

        final GoogleCredential googleCredential;
        try (ReadPathAccessor accessor = new FileChooserPathAccessor(settings.m_configFile, portObjectConnection)) {
            final var paths = accessor.getPaths(s -> {
            });
            if (paths.isEmpty()) {
                throw new InvalidSettingsException("Invalid path selected");
            }
            final var path = paths.get(0);
            try (InputStream inputStream = FSFiles.newInputStream(path)) {
                final var parser = new JsonObjectParser(GoogleApiUtil.getJsonFactory());
                final var fileContents = parser.parseAndClose(inputStream, StandardCharsets.UTF_8, GenericJson.class);
                try {
                    final var fromJson = fromJson(fileContents, awsSupplier);
                    googleCredential = new GoogleCredential(fromJson);
                } catch (ClassCastException | IllegalArgumentException e) {
                    throw new InvalidSettingsException("An invalid input stream was provided.", e);
                }
            }
        }

        m_credentialCacheKey = CredentialCache.store(googleCredential);
        return new PortObject[]{
            new CredentialPortObject(new CredentialPortObjectSpec(googleCredential.getType(), m_credentialCacheKey))};
    }

    /**
     * Copied and adapted from
     * {@link com.google.auth.oauth2.ExternalAccountCredentials#fromJson(Map, HttpTransportFactory)}
     */
    @SuppressWarnings("unchecked")
    private static ExternalAccountCredentials fromJson(final Map<String, Object> json,
        final Optional<AwsSecurityCredentialsSupplier> awsSupplier) throws InvalidSettingsException {
        ObjectUtils.allNotNull(json);

        String audience = (String) json.get("audience");
        String subjectTokenType = (String) json.get("subject_token_type");
        String tokenUrl = (String) json.get("token_url");

        Map<String, Object> credentialSourceMap = (Map<String, Object>) json.get("credential_source");

        // Optional params.
        String serviceAccountImpersonationUrl =
            getOptional(json, "service_account_impersonation_url", String.class);
        String tokenInfoUrl = getOptional(json, "token_info_url", String.class);
        String clientId = getOptional(json, "client_id", String.class);
        String clientSecret = getOptional(json, "client_secret", String.class);
        String quotaProjectId = getOptional(json, "quota_project_id", String.class);
        String userProject = getOptional(json, "workforce_pool_user_project", String.class);
        String universeDomain = getOptional(json, "universe_domain", String.class);
        Map<String, Object> impersonationOptionsMap =
            getOptional(json, "service_account_impersonation", Map.class);

        if (impersonationOptionsMap == null) {
          impersonationOptionsMap = new HashMap<String, Object>();
        }
        if (credentialSourceMap == null) {
            throw new InvalidSettingsException(
                "Invalid configuration file, could find any credential_source information.");
        }

        if (isAwsCredential(credentialSourceMap)) {
            return AwsCredentials.newBuilder().setHttpTransportFactory(GoogleApiUtil::getHttpTransport)
                .setAudience(audience).setSubjectTokenType(subjectTokenType).setTokenUrl(tokenUrl)
                .setTokenInfoUrl(tokenInfoUrl).setServiceAccountImpersonationUrl(serviceAccountImpersonationUrl)
                .setQuotaProjectId(quotaProjectId).setClientId(clientId).setClientSecret(clientSecret)
                .setServiceAccountImpersonationOptions(impersonationOptionsMap).setUniverseDomain(universeDomain)
                .setAwsSecurityCredentialsSupplier(awsSupplier
                    .orElseThrow(() -> new InvalidSettingsException("AWS Connection Information required as input")))
                .build();
        } else if (isPluggableAuthCredential(credentialSourceMap)) {
          return PluggableAuthCredentials.newBuilder()
              .setHttpTransportFactory(GoogleApiUtil::getHttpTransport)
              .setAudience(audience)
              .setSubjectTokenType(subjectTokenType)
              .setTokenUrl(tokenUrl)
              .setTokenInfoUrl(tokenInfoUrl)
              .setCredentialSource(new PluggableAuthCredentialSource(credentialSourceMap))
              .setServiceAccountImpersonationUrl(serviceAccountImpersonationUrl)
              .setQuotaProjectId(quotaProjectId)
              .setClientId(clientId)
              .setClientSecret(clientSecret)
              .setWorkforcePoolUserProject(userProject)
              .setServiceAccountImpersonationOptions(impersonationOptionsMap)
              .setUniverseDomain(universeDomain)
              .build();
        }
        return IdentityPoolCredentials.newBuilder()
            .setHttpTransportFactory(GoogleApiUtil::getHttpTransport)
            .setAudience(audience)
            .setSubjectTokenType(subjectTokenType)
            .setTokenUrl(tokenUrl)
            .setTokenInfoUrl(tokenInfoUrl)
            .setCredentialSource(new IdentityPoolCredentialSource(credentialSourceMap))
            .setServiceAccountImpersonationUrl(serviceAccountImpersonationUrl)
            .setQuotaProjectId(quotaProjectId)
            .setClientId(clientId)
            .setClientSecret(clientSecret)
            .setWorkforcePoolUserProject(userProject)
            .setServiceAccountImpersonationOptions(impersonationOptionsMap)
            .setUniverseDomain(universeDomain)
            .build();
      }

    /**
     * Copied from
     * {@link com.google.auth.oauth2.ExternalAccountCredentials#getOptional()}
     */
    private static <T> T getOptional(final Map<String, Object> json, final String fieldName, final Class<T> clazz) {
      Object value = json.get(fieldName);
      if (value == null || Data.isNull(value)) {
        return null;
      }
      return clazz.cast(value);
    }

    /**
     * Copied from
     * {@link com.google.auth.oauth2.ExternalAccountCredentials#isPluggableAuthCredential()}
     */
    private static boolean isPluggableAuthCredential(final Map<String, Object> credentialSource) {
      // Pluggable Auth is enabled via a nested executable field in the credential source.
      return credentialSource.containsKey("executable");
    }

    /**
     * Copied from
     * {@link com.google.auth.oauth2.ExternalAccountCredentials#isAwsCredential()}
     */
    private static boolean isAwsCredential(final Map<String, Object> credentialSource) {
      return credentialSource.containsKey("environment_id")
          && ((String) credentialSource.get("environment_id")).startsWith("aws");
    }

    @Override
    protected final void onDispose() {
        reset();
    }

    @Override
    protected void reset() {
        if (m_credentialCacheKey != null) {
            CredentialCache.delete(m_credentialCacheKey);
            m_credentialCacheKey = null;
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        setWarningMessage("Credential not available anymore. Please re-execute this node.");
    }
}
