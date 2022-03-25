def call(ghprbActualCommit, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription, credentialsId) {

    def TIDB_BRANCH = "release-4.0"
    def TIKV_BRANCH = "release-4.0"
    def PD_BRANCH = "release-4.0"
    def TICDC_BRANCH = "release-4.0"

    def kafka_version = "kafka_2.12-2.7.0"

    // parse tidb branch
    def m1 = ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m1) {
        TIDB_BRANCH = "${m1[0][1]}"
    }
    m1 = null
    println "TIDB_BRANCH=${TIDB_BRANCH}"

    // parse pd branch
    def m2 = ghprbCommentBody =~ /pd\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m2) {
        PD_BRANCH = "${m2[0][1]}"
    }
    m2 = null
    println "PD_BRANCH=${PD_BRANCH}"

    // parse tikv branch
    def m3 = ghprbCommentBody =~ /tikv\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m3) {
        TIKV_BRANCH = "${m3[0][1]}"
    }
    m3 = null
    println "TIKV_BRANCH=${TIKV_BRANCH}"

    // parse ticdc branch
    def m4 = ghprbCommentBody =~ /ticdc\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m4) {
        TICDC_BRANCH = "${m4[0][1]}"
    }
    m4 = null
    println "TICDC_BRANCH=${TICDC_BRANCH}"
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
            stage('Prepare') {
                node(label) {
                    println "${NODE_NAME}"
                    container("java") {
                        dir("/home/jenkins/agent/git/tibigdata") {

                            if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                                deleteDir()
                            }
                            checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: credentialsId, refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:tidb-incubator/TiBigData.git']]]
                            sh "git checkout -f ${ghprbActualCommit}"

                            stash includes: "**", name: "tibigdata"
                        }

                    }
                }
            }

            stage('Test') {
                def java_8_modules = ["flink/flink-1.11", "flink/flink-1.12", "flink/flink-1.13", "flink/flink-1.14", "mapreduce/mapreduce-base", "prestodb", "jdbc/driver", "ticdc", "jdbc/mariadb-compat"]
                def java_11_modules = ["prestosql", "trino"]

                groovy.lang.Closure run_integration_test = { module, isJava8 ->
                    node(label) {
                        println "${NODE_NAME}"
                        container("java") {
                            unstash 'tibigdata'

                            dir("/home/jenkins/agent/lib") {
                                sh "curl https://download.pingcap.org/jdk-11.0.12_linux-x64_bin.tar.gz | tar xz"

                                sh(script: """
                                        set -e
                                        set -x  
                                        rm -rf /maven/.m2/repository/*
                                        rm -rf /maven/.m2/settings.xml
                                        rm -rf ~/.m2/settings.xml
                    
                                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tibigdata/cache/tibigdata-m2-cache-latest.tar.gz
                                        curl -sL \$archive_url | tar -zx -C /maven
                                        """, returnStatus: true)
                            }

                            dir("/home/jenkins/agent/git/tibigdata/_run") {
                                sh "rm -rf *"

                                // tidb
                                def tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
                                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"
                                // tikv
                                def tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz | tar xz"
                                // pd
                                def pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"
                                //ticdc
                                def ticdc_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/ticdc/${TICDC_BRANCH}/sha1").trim()
                                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/ticdc/${ticdc_sha1}/centos7/ticdc-linux-amd64.tar.gz | tar xz"
                                // kafka
                                sh "curl ${FILE_SERVER_URL}/download/${kafka_version}.tgz | tar xz"
                                sh "mv ${kafka_version} kafka/"

                                sh """
                                        set -e
                                        set -x
                                        killall -9 tidb-server || true
                                        killall -9 tikv-server || true
                                        killall -9 pd-server || true
                                        killall -9 cdc || true
                                        killall -9 java || true
                                        sleep 10
                                        bin/pd-server --name=pd --data-dir=pd --config=../.ci/config/pd.toml &>pd.log &
                                        sleep 10
                                        bin/tikv-server --pd=127.0.0.1:2379 -s tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 --config=../.ci/config/tikv.toml &>tikv.log &
                                        sleep 10
                                        ps aux | grep '-server' || true
                                        curl -s 127.0.0.1:2379/pd/api/v1/status
                                        bin/tidb-server --store=tikv --path="127.0.0.1:2379" --config=../.ci/config/tidb.toml &>tidb.log &
                                        sleep 60
                                        cat _run/pd.log
                                    """

                                sh """
                                        rm -rf /tmp/zookeeper
                                        rm -rf /tmp/kafka-logs
                                        kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties &
                                        sleep 10
                                        kafka/bin/kafka-server-start.sh kafka/config/server.properties &
                                        sleep 10
                                        kafka/bin/kafka-topics.sh --create --topic tidb_test --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
                                        kafka/bin/kafka-topics.sh --describe --topic tidb_test --bootstrap-server localhost:9092
                                    """

                                sh """
                                        cd ticdc-linux-amd64
                                        ./bin/cdc server --pd="http://127.0.0.1:2379"  --log-file=ticdc.log --addr="0.0.0.0:8301" --advertise-addr="127.0.0.1:8301" &
                                        sleep 10
                                        ./bin/cdc cli changefeed create --pd="http://127.0.0.1:2379" --sink-uri="kafka://127.0.0.1:9092/tidb_test" --no-confirm
                                    """
                            }

                            dir("/home/jenkins/agent/git/tibigdata") {
                                try {
                                    java_home = ""

                                    if (!isJava8) {
                                        java_home = "export JAVA_HOME=/home/jenkins/agent/lib/jdk-11.0.12"
                                    }

                                    timeout(120) {
                                        sh """
                                        set -x
                                        set -euo pipefail
                                        export TIDB_HOST="127.0.0.1"
                                        export TIDB_PORT="4000"
                                        export TIDB_USER="root"
                                        export TIDB_PASSWORD=""
                                        $java_home
                                        mvn clean test-compile failsafe:integration-test -am -pl ${module}
                                    """
                                    }
                                } catch (err) {
                                    sh """
                            ps aux | grep '-server' || true
                            curl -s 127.0.0.1:2379/pd/api/v1/status || true
                            """
                                    sh "cat _run/pd.log"
                                    sh "cat _run/tikv.log"
                                    sh "cat _run/tidb.log"
                                    sh "cat _run/kafka/logs/server.log"
                                    sh "cat _run/ticdc-linux-amd64/ticdc.log"
                                    throw err
                                }
                            }
                        }
                    }
                }

                tests = [:]
                for (int i = 0; i < java_8_modules.size(); i++) {
                    module = java_8_modules.get(i)
                    tests[module] = { run_integration_test(module, true) }
                }

                for (int i = 0; i < java_11_modules.size(); i++) {
                    module = java_11_modules.get(i)
                    tests[module] = { run_integration_test(module, true) }
                }

                parallel tests
            }


            currentBuild.result = "SUCCESS"
        }
    }

    stage('Summary') {
        def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
        def msg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" + "${ghprbPullLink}" + "\n" + "${ghprbPullDescription}" + "\n" + "Integration Common Test Result: `${currentBuild.result}`" + "\n" + "Elapsed Time: `${duration} mins` " + "\n" + "${env.RUN_DISPLAY_URL}"

        print msg
    }
}


return this
