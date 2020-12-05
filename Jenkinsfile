pipeline {
     agent {
         docker {
             image env.DOCKER_FARM_IMAGE
             label env.DOCKER_FARM_LABEL
             args  env.DOCKER_FARM_ARGS
         }
    }
    options {
        gitLabConnection(env.gitlabConnection)
        gitlabBuilds(builds: ['checkout', 'clean', 'p3c-check', 'compile',  'test'])
        //ansiColor('xterm')
        timestamps()
    }
    environment{
          SERVICE_NAME='yig'
    }

    stages {
        stage('checkout') {
            post {
                success { updateGitlabCommitStatus name: 'checkout', state: 'success' }
                failure { updateGitlabCommitStatus name: 'checkout', state: 'failed' }
            }
            steps {
                script{
                    checkoutDependOnEnv env
                }
            }
        }
        stage('sonar-check') {
          post {
                 success {
                     updateGitlabCommitStatus name: 'sonar', state: 'success'
                 }
                 failure {
                     updateGitlabCommitStatus name: 'sonar', state: 'failed'
                 }
          }
          steps {
                 sh """
                     /home/sonar-scanner/bin/sonar-scanner
                 """
          }
        }
    }
}