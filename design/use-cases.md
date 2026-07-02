# Batch Queue Use Cases

- [Design](../README.md)

## Use Case: Successful Batch

### User submits manifest with 3 items

```mermaid
graph TD
  accTitle: 'New Batch is created with a status of Pending'
  Manifest[/Batch Manifest/]
  Ingest(Ingest Service)
  Batch[Batch: Pending]
  Manifest --> Ingest
  Ingest --> Batch
```

### Batch Queue starts Batch

```mermaid
graph TD
  accTitle: 'Batch status is processing'
  Batch[Batch: Processing]
```

### Batch downloads manifest and creates 3 jobs

```mermaid
graph TD
  accTitle: 'Batch (Processing) has 3 Jobs in a Pending state'
  Batch[Batch: Processing]
  Job1[Job 1: Pending]
  Job2[Job 2: Pending]
  Job3[Job 3: Pending]
  Batch --> |job1_payload_url| Job1
  Batch --> |job2_payload_url| Job2
  Batch --> |job3_payload_url| Job3
```

### Jobs Begin
  
```mermaid
graph TD
  accTitle: 'Batch (Processing) has 3 Jobs (Processing, Processing, Processing)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: Processing]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
```

### Job 2 Completes
  
```mermaid
graph TD
  accTitle: 'Batch (Processing) has 3 Jobs (Processing, Complete, Processing)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- |notify| Job2
  Batch --- Job3
```
### Job 3 completes  
```mermaid
graph TD
  accTitle: 'Batch (Processing) has 3 Jobs (Processing, Complete, Complete)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: COMPLETE]
  Batch --- Job1
  Batch --- Job2
  Batch --- |notify| Job3
```

### Job 1 completes  
```mermaid
graph TD
  accTitle: 'Batch (Reporting) has 3 Jobs (Complete, Complete, Complete): The completion of the last job triggers a state update for the Batch'
  Batch[Batch: Reporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: COMPLETE]
  Batch --- |notify| Job1
  Batch --- Job2
  Batch --- Job3
```

### Batch Reports Job Status to Depositor 
```mermaid
graph TD
  accTitle: 'Email notification is generated for the entire batch'
  Batch[Batch: Reporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: COMPLETE]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Email(Email Status to Depositor)
  Batch --> StatusReport
  StatusReport --> Email
```

### Batch Completes
```mermaid
graph TD
  accTitle: 'Batch is in a completed state'
  Batch[Batch: COMPLETED]
```

##  Use Case: Failed Batch

### User submits manifest with 3 items

```mermaid
graph TD
  accTitle: 'New Batch is created with a status of Pending'
  Manifest[/Batch Manifest/]
  Ingest(Ingest Service)
  Batch[Batch: Pending]
  Manifest --> Ingest
  Ingest --> Batch
```

### Batch Queue starts Batch

```mermaid
graph TD
  accTitle: 'Batch is in a Processing State'
  Batch[Batch: Processing]
```

### Batch downloads manifest and creates 3 jobs

```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Pending, Pending, Pending)'
  Batch[Batch: Processing]
  Job1[Job 1: Pending]
  Job2[Job 2: Pending]
  Job3[Job 3: Pending]
  Batch --> |job1_payload_url| Job1
  Batch --> |job2_payload_url| Job2
  Batch --> |job3_payload_url| Job3
```

### Jobs Begin
  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Processing, Processing)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: Processing]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
```

### Job 2 Completes
  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Complete, Processing)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- |notify| Job2
  Batch --- Job3
```
### Job 3 fails  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Complete, Failed)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- |notify| Job3
```

### Job 1 completes  
```mermaid
graph TD
  accTitle: 'Batch(Reporting) has 3 Jobs (Complete, Complete, Failed): Batch goes to Reporting state upon completion of the last job'
  Batch[Batch: Reporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- |notify| Job1
  Batch --- Job2
  Batch --- Job3
```

### Batch Reports Job Status to Depositor 
```mermaid
graph TD
  accTitle: 'Email notification is generated for the entire batch'
  Batch[Batch: Reporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Email(Email Status to Depositor)
  Batch --> StatusReport
  StatusReport --> Email
```

### Batch Goes to Failed state
```mermaid
graph TD
  accTitle: 'Batch(Failed) has 3 Jobs (Complete, Complete, Failed): After reporting the failure, batch status changes to Failed'
  Batch[Batch: Failed]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```


### Admin Deletes Batch after determining that resubmission of failed job is not possible
```mermaid
graph TD
  accTitle: 'Batch State is deleted'
  Batch[Batch: DELETED]
```


## Use Case: Failed Batch with Successful Retry


### User submits manifest with 3 items

```mermaid
graph TD
  accTitle: 'New Batch is created with a status of Pending'
  Manifest[/Batch Manifest/]
  Ingest(Ingest Service)
  Batch[Batch: Pending]
  Manifest --> Ingest
  Ingest --> Batch
```

### Batch Queue starts Batch

```mermaid
graph TD
  accTitle: 'Batch is in a Processing State'
  Batch[Batch: Processing]
```

### Batch downloads manifest and creates 3 jobs

```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Pending, Pending, Pending)'
  Batch[Batch: Processing]
  Job1[Job 1: Pending]
  Job2[Job 2: Pending]
  Job3[Job 3: Pending]
  Batch --> |job1_payload_url| Job1
  Batch --> |job2_payload_url| Job2
  Batch --> |job3_payload_url| Job3
```

### Jobs Begin
  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Processing, Processing)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: Processing]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
```

### Job 2 Completes
  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Complete, Processing)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- |notify| Job2
  Batch --- Job3
```
### Job 3 fails  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Complete, Failed)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- |notify| Job3
```

### Job 1 completes  
```mermaid
graph TD
  accTitle: 'Batch(Reporting) has 3 Jobs (Complete, Complete, Failed): Batch state changes when the last job completed'
  Batch[Batch: Reporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- |notify| Job1
  Batch --- Job2
  Batch --- Job3
```

### Batch Reports Job Status to Depositor 
```mermaid
graph TD
  accTitle: 'Email notification is generated for the entire batch'
  Batch[Batch: Reporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Email(Email Status to Depositor)
  Batch --> StatusReport
  StatusReport --> Email
```

### Batch Goes to Failed state
```mermaid
graph TD
  accTitle: 'Batch(Failed) has 3 Jobs (Complete, Complete, Failed)'
  Batch[Batch: Failed]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```

### Job 3 is restarted
```mermaid
graph TD
  accTitle: 'Batch(Failed) has 3 Jobs (Complete, Complete, Processing)'
  Batch[Batch: Failed]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```

### Job 3 completes
```mermaid
graph TD
  accTitle: 'Batch(Failed) has 3 Jobs (Complete, Complete, Complete)'
  Batch[Batch: Failed]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: COMPLETE]
  Batch --- Job1
  Batch --- Job2
  Batch --- |notify| Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```

### Admin changes batch state to UpdateReporting
```mermaid
graph TD
  accTitle: 'Batch(UpdateReporting) has 3 Jobs (Complete, Complete, Complete)'
  Batch[Batch: UpdateReporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: COMPLETE]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```

### Email is sent to depositor showing the status change for Job 3
```mermaid
graph TD
  accTitle: 'Email notification is generated for the entire batch'
  Batch[Batch: UpdateReporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: COMPLETE]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --> StatusReport
  StatusReport -.-> Batch
  Email(Email Status to Depositor)
  StatusReport --> Email
```

### Batch Status is COMPLETE
```mermaid
graph TD
  accTitle: 'Batch(Complete)'
  Batch[Batch: COMPLETE]
```

## Use Case: Failed Batch with Unsuccessful Retry


### User submits manifest with 3 items

```mermaid
graph TD
  accTitle: 'New Batch is created with a status of Pending'
  Manifest[/Batch Manifest/]
  Ingest(Ingest Service)
  Batch[Batch: Pending]
  Manifest --> Ingest
  Ingest --> Batch
```

### Batch Queue starts Batch

```mermaid
graph TD
  accTitle: 'Batch is in a Processing State'
  Batch[Batch: Processing]
```

### Batch downloads manifest and creates 3 jobs

```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Pending, Pending, Pending)'
  Batch[Batch: Processing]
  Job1[Job 1: Pending]
  Job2[Job 2: Pending]
  Job3[Job 3: Pending]
  Batch --> |job1_payload_url| Job1
  Batch --> |job2_payload_url| Job2
  Batch --> |job3_payload_url| Job3
```

### Jobs Begin
  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Processing, Processing)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: Processing]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
```

### Job 2 Completes
  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Complete, Processing)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- |notify| Job2
  Batch --- Job3
```
### Job 3 fails  
```mermaid
graph TD
  accTitle: 'Batch(Processing) has 3 Jobs (Processing, Complete, Failed)'
  Batch[Batch: Processing]
  Job1[Job 1: Processing]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- |notify| Job3
```

### Job 1 completes  
```mermaid
graph TD
  accTitle: 'Batch(Reporting) has 3 Jobs (Complete, Complete, Failed): Batch state changes when the last job completes'
  Batch[Batch: Reporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- |notify| Job1
  Batch --- Job2
  Batch --- Job3
```

### Batch Reports Job Status to Depositor 
```mermaid
graph TD
  accTitle: 'Email notification is generated for the entire batch'
  Batch[Batch: Reporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Email(Email Status to Depositor)
  Batch --> StatusReport
  StatusReport --> Email
```

### Batch Goes to Failed state
```mermaid
graph TD
  accTitle: 'Batch(Failed) has 3 Jobs (Complete, Complete, Failed)'
  Batch[Batch: Failed]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```

### Job 3 is restarted
```mermaid
graph TD
  accTitle: 'Batch(Failed) has 3 Jobs (Complete, Complete, Processing)'
  Batch[Batch: Failed]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Processing]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```

### Job 3 fails again
```mermaid
graph TD
  accTitle: 'Batch(Failed) has 3 Jobs (Complete, Complete, Failed)'
  Batch[Batch: Failed]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- |notify| Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```

### Admin changes batch state to UpdateReporting
```mermaid
graph TD
  accTitle: 'Batch(UpdateReporting) has 3 Jobs (Complete, Complete, Failed)'
  Batch[Batch: UpdateReporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --- StatusReport
```

### Since there is no job state change since last report, no email is sent
```mermaid
graph TD
  accTitle: 'Batch(UpdateReporting) has 3 Jobs (Complete, Complete, Failed)'
  Batch[Batch: UpdateReporting]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --> StatusReport
  StatusReport -.-> Batch
```

### Batch Status is Failed
```mermaid
graph TD
  accTitle: 'Batch(Failed) has 3 Jobs (Complete, Complete, Failed)'
  Batch[Batch: Failed]
  Job1[Job 1: COMPLETE]
  Job2[Job 2: COMPLETE]
  Job3[Job 3: Failed]
  Batch --- Job1
  Batch --- Job2
  Batch --- Job3
  StatusReport[/StatusReport/]
  Batch --> StatusReport
```

### Admin Deletes Batch after determining that resubmission of failed job is not possible
```mermaid
graph TD
  accTitle: 'Batch(Deleted)'
  Batch[Batch: DELETED]
```
