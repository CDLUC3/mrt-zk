# Test Plan

## Scale testing
- Large number of small objects submitted as a Job Manifest
-- This will test ZK interactions with multiple processes and locks
- Small number of very large Jobs
-- This will test for timeout conditions

## Batch testing
- Errors in Batch creation should fail all jobs and notify

## Failure testing
- Jobs set to fail are represented correctly in ZK batch and queue nodes
- Ensure that failed jobs release all ZK locks

## Transition/Rules Testing
- Test hold collection before starting batch
- Test hold collection as a large batch is processing
- Release job while collection is held
- Release job after collection hold is released
- Release all jobs for a collection
- Requeue job failure for each phase (document how to force failures)
  - Estimating (temp hardcode)
  - Provisioning (temp hardcode)
  - Downloading (bad manifest url)
  - Processing (bad storage node)
  - Recording (ask David, turn off inventory and manually fail the step)
  - Notify (possibly bad email or callback)
