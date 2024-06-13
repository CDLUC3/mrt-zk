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
