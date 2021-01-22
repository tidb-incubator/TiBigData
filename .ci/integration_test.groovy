def call(ghprbActualCommit, ghprbCommentBody, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription, credentialsId) {

    catchError {
        node ('build') {
            container("java") {
                stage('Prepare') {
                    dir("/home/jenkins/agent/git/tibigdata") {
                        sh """
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

                stage('Test') {
                    dir("/home/jenkins/agent/git/tibigdata") {
                        sh """
                        mvn clean test -am -pl flink
                        mvn clean test -am -pl prestodb
                        mvn clean test -am -pl jdbc
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
        "Integration Common Test Result: `${currentBuild.result}`" + "\n" +
        "Elapsed Time: `${duration} mins` " + "\n" +
        "${env.RUN_DISPLAY_URL}"

        print msg
    }
}

return this
