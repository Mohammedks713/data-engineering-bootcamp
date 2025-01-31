# Data Pipeline Management Plan

## 1. Ownership of Pipelines

### Primary and Secondary Owners

* Note: I am referring to the owners responsible for keeping the underlying data for the metrics current
and up to date. The Data Scientist or Business stakeholder owns the actual metric/analytic.

| **Pipeline**                               | **Primary Owner** | **Secondary Owner** |
|-------------------------------------------|-------------------|---------------------|
| Profit                                    | Data Engineer A   | Data Engineer B     |
| Unit-level profit needed for experiments  | Data Engineer B   | Data Engineer C     |
| Aggregate profit reported to investors    | Data Engineer C   | Data Engineer A     |
| Growth                                    | Data Engineer D   | Data Engineer A     |
| Aggregate growth reported to investors    | Data Engineer A   | Data Engineer D     |
| Daily growth needed for experiments       | Data Engineer B   | Data Engineer C     |
| Engagement                                | Data Engineer C   | Data Engineer D     |
| Aggregate engagement reported to investors| Data Engineer D   | Data Engineer B     |

## 2. On-Call Schedule

### Weekly Rotations
Each data engineer will take one week of on-call duties, cycling through the team in the following order:
1. Data Engineer A
2. Data Engineer B
3. Data Engineer C
4. Data Engineer D

### Holidays
- On-call schedules will adjust for national holidays, ensuring that no one is assigned on-call duties during their vacation or public holidays in their region.
- Swap requests between engineers must be submitted one week in advance.
- During major holiday seasons, engineers may split daily on-call responsibilities to reduce individual workload.

## 3. Run Books

Run books will be created for all pipelines reporting metrics to investors. These include:

### Aggregate Profit Reported to Investors
**Steps to Resolve Issues:**
1. Check data sources for missing or delayed inputs (e.g., sales databases).
2. Validate ETL job logs for errors or timeouts.
3. Confirm data aggregation scripts ran successfully.
4. Notify stakeholders if metrics cannot be delivered on time.

### Aggregate Growth Reported to Investors
**Steps to Resolve Issues:**
1. Inspect raw growth data sources for anomalies.
2. Verify scheduled jobs for aggregating growth metrics.
3. Review job history for processing delays or errors.
4. Escalate to the secondary owner if primary troubleshooting steps fail.

### Aggregate Engagement Reported to Investors
**Steps to Resolve Issues:**
1. Check engagement data pipelines for any missing events.
2. Confirm transformations align with business logic.
3. Run backfill jobs if recent data is unavailable.
4. Notify team and stakeholders of any delays.

## 4. Potential Issues in Pipelines

### General Issues
- **Data Source Unavailability**: One or more data sources fail to provide timely updates.
- **Schema Changes**: Changes in upstream data schemas break ETL jobs.
- **Processing Failures**: Errors during data transformation or aggregation.
- **Pipeline Latency**: Jobs running slower than expected, causing downstream delays.
- **Storage Limitations**: Insufficient storage space for intermediary or output data.

### Specific Risks
- **Profit Pipelines**:
  - Inaccurate revenue or cost data due to upstream errors.
  - Delayed unit-level data impacting real-time experiments.

- **Growth Pipelines**:
  - Incorrect growth rate calculations due to incomplete data.
  - Misalignment between daily and aggregate growth numbers.

- **Engagement Pipelines**:
  - Missing user engagement events from tracking systems.
  - Errors in aggregating engagement metrics over time.

## Notes
- All engineers should be familiar with the run books for investor-related pipelines.
- A regular review of potential issues and lessons learned from past incidents will help improve the overall reliability of the pipelines.
