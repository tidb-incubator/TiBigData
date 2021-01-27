def call(ghprbActualCommit, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription, credentialsId) {

    def TIDB_BRANCH = "release-4.0"
    def TIKV_BRANCH = "release-4.0"
    def PD_BRANCH = "release-4.0"

    // parse tidb branch
    def m1 = ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m1) {
        TIDB_BRANCH = "${m1[0][1]}"
    }
    println "TIDB_BRANCH=${TIDB_BRANCH}"

    // parse pd branch
    def m2 = ghprbCommentBody =~ /pd\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m2) {
        PD_BRANCH = "${m2[0][1]}"
    }
    println "PD_BRANCH=${PD_BRANCH}"

    // parse tikv branch
    def m3 = ghprbCommentBody =~ /tikv\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m3) {
        TIKV_BRANCH = "${m3[0][1]}"
    }
    println "TIKV_BRANCH=${TIKV_BRANCH}"

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

                    dir("/home/jenkins/agent/git/tibigdata/_run") {
                        // tidb
                        def tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
                        sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"
                        // tikv
                        def tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                        sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz | tar xz"
                        // pd
                        def pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                        sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"

                        sh """
                        killall -9 tidb-server || true
                        killall -9 tikv-server || true
                        killall -9 pd-server || true
                        killall -9 java || true
                        sleep 10
                        bin/pd-server --name=pd --data-dir=pd --config=../.ci/config/pd.toml &>pd.log &
                        sleep 10
                        bin/tikv-server --pd=127.0.0.1:2379 -s tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 --config=../.ci/config/tikv.toml &>tikv.log &
                        sleep 10
                        ps aux | grep '-server' || true
                        curl -s 127.0.0.1:2379/pd/api/v1/status || true
                        bin/tidb-server --store=tikv --path="127.0.0.1:2379" --config=../.ci/config/tidb.toml &>tidb.log &
                        sleep 60
                        """
                    }
                }

                stage('Test') {
                    dir("/home/jenkins/agent/git/tibigdata") {
                        try {
                            sh """
                            mvn clean test -am -pl flink
                            mvn clean test -am -pl prestodb
                            mvn clean test -am -pl jdbc
                            """
                        } catch (err) {
                            sh """
                            ps aux | grep '-server' || true
                            curl -s 127.0.0.1:2379/pd/api/v1/status || true
                            """
                            sh "cat _run/pd.log"
                            sh "cat _run/tikv.log"
                            sh "cat _run/tidb.log"
                            throw err
                        }
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
