// Tell Jenkins how to build projects from this repository
pipeline {

    parameters {
        // DISTRIBUTION
        booleanParam(defaultValue: false, description: 'Set to true if you want to create a release', name: 'RELEASE')
    }

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
        jdk 'OpenJDK 8'
    }

    stages {
        stage('Clean') {
            steps {
                sh "./gradlew clean"
            }
        }

        stage('Build') {
            steps {
                sh "./gradlew build"
            }
        }

        stage('Release') {
            when {
                expression { params.RELEASE == true }
            }
            steps {
                sh "./gradlew clean collectGithubRelease -Prelease=true"
                withCredentials([usernamePassword(
                        credentialsId: 'github-access-token',
                        passwordVariable: 'GITHUB_RELEASE_TOKEN',
                        usernameVariable: 'GITHUB_USER')]) {
                    sh "./gradlew githubRelease -Prelease=true -PgithubToken=${GITHUB_RELEASE_TOKEN}"
                }
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
