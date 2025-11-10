#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2026-06'

library "knime-pipeline@$BN"

properties([
    // provide a list of upstream jobs which should trigger a rebuild of this job
    pipelineTriggers([
        upstream("knime-aws/${BRANCH_NAME.replaceAll('/', '%2F')}" +
            ", knime-google/${env.BRANCH_NAME.replaceAll('/', '%2F')}")
    ]),
    parameters(workflowTests.getConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

try {
    // provide the name of the update site project
    knimetools.defaultTychoBuild('org.knime.update.google.wif')


    workflowTests.runTests(
        dependencies: [
            repositories: [
                'knime-aws',
                'knime-bigdata',
                'knime-bigdata-externals',
                'knime-cloud',
                'knime-credentials-base',
                'knime-database',
                'knime-database-proprietary',
                'knime-database-bigquery',
                'knime-filehandling',
                'knime-gateway',
                'knime-google',
                'knime-google-wif',
                'knime-kerberos',
                'knime-office365',
                'knime-testing-internal'
            ],
            ius: [
                'org.knime.features.database.community.hub.driver.feature.group'
            ]
        ]
    )
    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}

/* vim: set shiftwidth=4 expandtab smarttab: */
