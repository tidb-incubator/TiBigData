def call(ghprbActualCommit, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription, credentialsId) {

    def label = "regression-test-tispark-${BUILD_NUMBER}"

    podTemplate(name: label, label: label, instanceCap: 12, namespace: 'jenkins-tispark', containers: [
            containerTemplate(name: 'java', image: 'hub.pingcap.net/jenkins/centos7_golang-1.13_java:cached',
                    resourceRequestCpu: '8000m',
                    resourceRequestMemory: '24Gi',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
    ]) {
        catchError {
            node(label) {
                container("java") {
                    stage('Prepare') {
                        dir("/home/jenkins/agent/git/tibigdata") {
                            sh """
                        rm -rf /maven/.m2/repository/*
                        rm -rf /maven/.m2/settings.xml
                        rm -rf ~/.m2/settings.xml
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/client-java/cache/tikv-client-java-m2-cache-latest.tar.gz
                        curl -sL \$archive_url | tar -zx -C /maven
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tibigdata/cache/tibigdata-m2-cache-latest.tar.gz
                        curl -sL \$archive_url | tar -zx -C /maven
                        """
                            if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                                deleteDir()
                            }
                            checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: credentialsId, refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:tidb-incubator/TiBigData.git']]]
                            sh "git checkout -f ${ghprbActualCommit}"
                        }
                        dir("/home/jenkins/agent/lib") {
                            sh "curl https://download.pingcap.org/jdk-11.0.12_linux-x64_bin.tar.gz | tar xz"
                        }
                    }

                    stage('Build') {
                        dir("/home/jenkins/agent/git/tibigdata") {
                            timeout(30) {
                                sh ".ci/build.sh"
                            }
                        }
                    }
                }
            }
            currentBuild.result = "SUCCESS"
        }
    }

    stage('Summary') {
        def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
        def msg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
                "${ghprbPullLink}" + "\n" +
                "${ghprbPullDescription}" + "\n" +
                "Build Result: `${currentBuild.result}`" + "\n" +
                "Elapsed Time: `${duration} mins` " + "\n" +
                "${env.RUN_DISPLAY_URL}"
        print msg
    }
}

return this
