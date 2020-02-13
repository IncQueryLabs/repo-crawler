// Tell Jenkins how to build projects from this repository
pipeline {

    agent {
        label 'node'
    }

    environment {
        CI = 'true'
        TEAMS_NOTIFICATION_URL = credentials('incquery-server-teams-notification-url')
    }

    options {
        // Keep only the last 15 builds
        buildDiscarder(logRotator(artifactNumToKeepStr: '3', numToKeepStr: '15'))
        // Do not execute the same pipeline concurrently
        disableConcurrentBuilds()
        // Display timestamps in log
        timestamps()
    }

    tools {
        jdk 'Oracle JDK 8'
    }

    stages {
        stage('Build') {
            steps {
                sh "./gradlew build"
            }
        }
    }

    post {
        always {
            script {
                archiveArtifacts '**/build/distributions/**, **/build/libs/**'
            }
        }
        success {
            office365ConnectorSend status: "Success",
                color: "00db00",
                webhookUrl: "${TEAMS_NOTIFICATION_URL}"
        }
        unstable {
            office365ConnectorSend status: "Unstable",
                color: "fcb019",
                webhookUrl: "${TEAMS_NOTIFICATION_URL}"
        }
        failure {
            office365ConnectorSend status: "Failure",
                color: "f21607",
                webhookUrl: "${TEAMS_NOTIFICATION_URL}"
        }
    }
}
