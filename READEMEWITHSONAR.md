<a href="https://www.sonarqube.org/"><img src="https://www.sonarqube.org/assets/logo-31ad3115b1b4b120f3d1efd63e6b13ac9f1f89437f0cf6881cc4d8b5603a52b4.svg" alt="Caddy" width="450"></a>
##  sonarQube
<p><a href="http://www.sonarqube.org/" target="_blank">SonarQube</a>® is an automatic code review tool to detect bugs, vulnerabilities and code smells in your code. It can integrate with your existing workflow to enable continuous code inspection across your project branches and pull requests.</p>

### QuickStart With Jenkins
We need to use CI to complete the check and pull of the code, here we use jenkins to complete these operations.First we need to set up our workflow on jenkins.

![New Item](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarQube1.png)

Then fill in your project name, then select Create a new pipeline below, then click OK

![New Item2](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarQube2.png)

After entering the settings, set some settings you need, set the method and identity entry for the pull item, it is worth noting that items marked with red arrows are required to be set.

![Setting1](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarQube3.png)
![Setting2](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarQube4.png)
![Setting3](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarQube5.png)
![Setting4](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarQube6.png)

When setting up Pipeline, you need to set the corresponding account and the corresponding script to pull the code. I won't go into details in this part. You can easily get them through Google.

![Setting5](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarQube7.png)

Finally, your pipeline project is set up, and then the token is bound to the gitlab pipeline setup to complete the initial configuration. Next we need to configure the Jenkins script file to tell CI what we need to do.

![Item](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarQube8.png)

The configuration file must be named Jenkinsfile, see Jenkins for details.

```editorconfig
pipeline {
     agent {
         docker {
             image env.DOCKER_FARM_IMAGE
             label env.DOCKER_FARM_LABEL
             args  env.DOCKER_FARM_ARGS
         }// Set up our packaged environment here
    }
    options {
        gitLabConnection(env.gitlabConnection)
        timestamps()
    }// Set environment connection
    environment{
          SERVICE_NAME='yig'
    }// Set the name, the above parameters actually do not work, just to complete the following inspection process, the real packaging settings will be much more complicated than this


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
        }// Check the code, mainly to scan our source code
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
          }// Static code check trigger, this is the part that ultimately implements the code check.
        }
    }
}
```

Then configure the configuration file of sonarQube

```editorconfig
# Connect to the account of the sonar tool. This parameter is provided by the sonarQube administrator and sometimes uses the password.
sonar.login=aba129a2f9df39f295cc56ab82db1c74e6d78266
# Project key, need to be guaranteed unique in sonarQube
sonar.projectKey=yig
# project name
sonar.projectName=yig
# Source code path
sonar.sources=.
sonar.exclusions=**/*_test.go,**/vendor/**
# Source code file encoding
sonar.sourceEncoding=UTF-8
# Specify unit test code path
sonar.language=go
sonar.tests=.
sonar.test.inclusions=**/*_test.go
sonar.test.exclusions=**/vendor/**
# Plugin configuration for pdf
sonar.pdf.username=admin
sonar.pdf.password=admin
```
In addition to the login, projectKey, projectName parameters, we use the default parameters, if there are other requirements, you can refer to the official website settings.

### Congratulations
your yig project will be able to automatically generate the corresponding inspection report when you push the code.

![UI](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarUI.png)
![Report](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarresult1.png)
![Report](https://oss-doc.oss-cn-north-1.unicloudsrv.com/images/sonarresult2.png)

