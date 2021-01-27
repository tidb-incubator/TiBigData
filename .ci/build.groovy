def call(ghprbActualCommit, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription, credentialsId) {
    
    catchError {
        node ('build') {
            container("java") {
                stage('Prepare') {
                    dir("/home/jenkins/agent/git/tibigdata") {
                        sh """
                        rm -rf /maven/.m2/repository/*
                        rm -rf /maven/.m2/settings.xml
                        rm -rf ~/.m2/settings.xml
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tibigdata/cache/tibigdata-m2-cache-latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                        """
                        if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                            deleteDir()
                        }
                        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: credentialsId, refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:tidb-incubator/TiBigData.git']]]
                        sh "git checkout -f ${ghprbActualCommit}"
                    }
                }

                stage('Build') {
                    dir("/home/jenkins/agent/git/tibigdata") {
                        sh """
                        mvn clean compile -am -pl flink
                        mvn clean compile -am -pl prestodb
                        mvn clean compile -am -pl jdbc
                        """
                    }
                }
            }
        }
        currentBuild.result = "SUCCESS"
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
