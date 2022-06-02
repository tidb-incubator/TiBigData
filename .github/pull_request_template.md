## What is the purpose of the change

*(For example: This pull request makes task deployment go through the blob server, rather than through RPC. That way we avoid re-transferring them on each deployment (during recovery).)*

## Brief change log

*(for example:)*
- *The TaskInfo is stored in the blob store on job creation time as a persistent artifact*
- *Deployments RPC transmits only the blob storage reference*
- *TaskManagers retrieve the TaskInfo from the blob cache*

## Verifying this change

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
- *Added integration tests for end-to-end deployment with large payloads (100MB)*
- *Extended integration test for recovery after master (JobManager) failure*
- *Added test that validates that TaskInfo is transferred only once across recoveries*
- *Manually verified the change by running a 4 node cluster with 2 JobManagers and 4 TaskManagers, a stateful streaming program, and killing one JobManager and two TaskManagers during the execution, verifying that recovery happens correctly.*

## Does this pull request potentially affect one of the following parts:

- Dependencies (does it add or upgrade a dependency): (yes / no)
- The public API: (yes / no)
- The runtime per-record code paths (performance sensitive): (yes / no / don't know)

## Documentation

- Does this pull request introduce a new feature? (yes / no)
- If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)
