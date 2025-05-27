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
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AwsCredentials;
import com.google.auth.oauth2.AwsSecurityCredentialsSupplier;
import com.google.auth.oauth2.ExternalAccountCredentials;

/**
 * The Databricks Workspace Connector node model.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
final class GoogleWIFNodeModel extends WebUINodeModel<GoogleWIFSettings> {

    private UUID m_credentialCacheKey;

    /**
     * @param portsConfig The node configuration.
     */
    GoogleWIFNodeModel(final PortsConfiguration portsConfig) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts(), GoogleWIFSettings.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs, final GoogleWIFSettings settings)
        throws InvalidSettingsException {
        final int awsPort = inSpecs.length == 2 ? 1 : 0;
        if (!(inSpecs[awsPort] instanceof CloudConnectionInformationPortObjectSpec)) {
            throw new InvalidSettingsException(
                "Incompatible input connection. Connect the Fabric Workspace Connector output port.");
        }

        return new PortObjectSpec[]{new CredentialPortObjectSpec()};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final GoogleWIFSettings settings) throws Exception {
        final CloudConnectionInformationPortObjectSpec awsSpec;
        final Optional<FSConnection> portObjectConnection;
        if (inObjects.length == 2) {
            final FileSystemPortObject portObject = (FileSystemPortObject)inObjects[0];
            portObjectConnection = portObject == null ? Optional.empty() : portObject.getFileSystemConnection();
            awsSpec = (CloudConnectionInformationPortObjectSpec)((AmazonConnectionInformationPortObject)inObjects[1])
                .getSpec();
        } else {
            portObjectConnection = Optional.empty();
            awsSpec = (CloudConnectionInformationPortObjectSpec)((AmazonConnectionInformationPortObject)inObjects[0])
                .getSpec();
        }

        final var awsSupplier = new AwsSupplier(awsSpec.getConnectionInformation());

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
        final AwsSecurityCredentialsSupplier awsSupplier) throws InvalidSettingsException {

        final var audience = (String)json.get("audience");
        final var subjectTokenType = (String)json.get("subject_token_type");
        final var tokenUrl = (String)json.get("token_url");

        final var credentialSourceMap = (Map<String, Object>)json.get("credential_source");

        // Optional params.
        final var serviceAccountImpersonationUrl = (String)json.get("service_account_impersonation_url");
        final var tokenInfoUrl = (String)json.get("token_info_url");
        final var clientId = (String)json.get("client_id");
        final var clientSecret = (String)json.get("client_secret");
        final var quotaProjectId = (String)json.get("quota_project_id");
        final var universeDomain = (String)json.get("universe_domain");
        var impersonationOptionsMap = (Map<String, Object>)json.get("service_account_impersonation");
        if (impersonationOptionsMap == null) {
            impersonationOptionsMap = new HashMap<String, Object>();
        }

        if (credentialSourceMap == null ) {
            throw new InvalidSettingsException("Provided config file is invalid");
        }

        if (credentialSourceMap.containsKey("environment_id")
                && ((String)credentialSourceMap.get("environment_id")).startsWith("aws")) {
            return AwsCredentials.newBuilder()
                .setHttpTransportFactory(GoogleApiUtil::getHttpTransport)
                .setAudience(audience).setSubjectTokenType(subjectTokenType).setTokenUrl(tokenUrl)
                .setTokenInfoUrl(tokenInfoUrl)
                .setServiceAccountImpersonationUrl(serviceAccountImpersonationUrl).setQuotaProjectId(quotaProjectId)
                .setClientId(clientId).setClientSecret(clientSecret)
                .setServiceAccountImpersonationOptions(impersonationOptionsMap).setUniverseDomain(universeDomain)
                .setAwsSecurityCredentialsSupplier(awsSupplier).build();
        }
        throw new InvalidSettingsException("Provided config file is not for AWS");
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
