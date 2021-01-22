def call(ghprbActualCommit, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription) {
    env.PATH = "${env.GOROOT}/bin:/home/jenkins/bin:/bin:${env.PATH}"
    
    catchError {
        node ('build') {
            container("java") {
                stage('Prepare') {
                    dir("/home/jenkins/agent/git/tibigdata") {
                        sh """
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tibigdata/cache/tibigdata-m2-cache-latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                        """
                    }
                }

                stage('Build') {
                    dir("/home/jenkins/agent/git/tibigdata") {
                        sh """
                        git checkout -f ${ghprbActualCommit}
                        mvn clean package -Dmaven.test.skip=true -am -pl flink
                        mvn clean package -Dmaven.test.skip=true -am -pl prestodb
                        mvn clean package -Dmaven.test.skip=true -am -pl jdbc
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