# Guidance for Migrating tabular data from Amazon S3 to S3 Tables Cost Estimates (us-east-2)

This document provides estimated costs for migrating data in this specific scenario to Amazon S3 Tables using AWS EMR in the us-east-2 region. Migration costs vary dependent on scenario, size of data, and number of objects to migrate.

The average object size in this data set for testing is roughly 70 MiB.

Public pricing for S3 and S3 Tables is located [here](https://aws.amazon.com/s3/pricing/) to assist in defining projected costs for your organization.  

## Cost Scenario Breakdown by Data Size and Cluster Configuration

### 1 TB Data, 24,075 S3 GETs, 8421 S3 Table PUTs

| Cluster Size | Processing Time | EC2 Compute | EMR Fees | Other Costs* | S3 GETs | S3 Table PUTs | Total Cost |
|--------------|-----------------|-------------|----------|--------------|---------|---------------|------------|
| Xlarge       | 00:09:01        | $32.76      | $8.15    | $2.50        | $0.24   | $0.04         | $43.69     |
| Large        | 00:18:46        | $51.17      | $1.44    | $5.00        | $0.24   | $0.04         | $57.89     |
| Medium       | 00:23:57        | $33.76      | $3.64    | $5.00        | $0.24   | $0.04         | $42.68     |
| Small        | 01:14:50        | $40.56      | $3.04    | $2.50        | $0.24   | $0.04         | $46.38     |

### 17 TB Data, 253,637 S3 GETs, 172,813 S3 Table PUTs

| Cluster Size | Processing Time | EC2 Compute | EMR Fees | Other Costs* | S3 GETs | S3 Table PUTs | Total Cost |
|--------------|-----------------|-------------|----------|--------------|---------|---------------|------------|
| Xlarge       | 02:02:23        | $444.83     | $110.69  | $25.00       | $2.54   | $0.86         | $583.92    |
| Large        | 03:43:03        | $472.60     | $13.30   | $25.00       | $2.54   | $0.86         | $514.30    |
| Medium       | 05:00:28        | $423.31     | $45.68   | $25.00       | $2.54   | $0.86         | $497.39    |
| Small        | 19:54:39        | $371.28     | $27.05   | $25.00       | $2.54   | $0.86         | $426.73    |

### 51 TB Data, 760,908	S3 GETs, 523,503 S3 Table PUTs

| Cluster Size | Processing Time | EC2 Compute | EMR Fees | Other Costs* | S3 GETs | S3 Table PUTs | Total Cost |
|--------------|-----------------|-------------|----------|--------------|---------|---------------|------------|
| Xlarge       | 05:55:40        | $1,290.44   | $320.96  | $50.00       | $7.61   | $2.62         | $1,671.63  |
| Large        | 10:42:45        | $1,360.31   | $38.29   | $50.00       | $7.61   | $2.62         | $1,458.83  |
| Medium       | 14:47:36        | $1,250.36   | $134.87  | $50.00       | $7.61   | $2.62         | $1,445.46  |
| Small        | 60:56:40        | $1,135.11   | $85.08   | $50.00       | $7.61   | $2.62         | $1,280.42  |

*Includes [CloudWatch Logs](https://aws.amazon.com/cloudwatch/pricing/), [Step Functions](https://aws.amazon.com/step-functions/pricing/), and [SNS](https://aws.amazon.com/sns/pricing/) aggregated estimates.

## Assumptions
- S3 Data Transfer and Storage costs not included
- EMR fees based on specified hourly rates per instance type
- us-east-2 pricing as of December 2024
## Additional Cost Considerations
- The S3 Table PUTs column does not include failed or orphaned PUTs.
- [S3 Data Transfer Out](https://aws.amazon.com/s3/pricing/) costs are dependent upon cross-region migrations and which regions are involved.
- There are no S3 Data Transfer Costs within the same region.
- The costs are only associated with the Data Migration and do not include ongoing monthly costs for Storage.
    - [S3 Tables](https://aws.amazon.com/s3/pricing/) monthly costs, outside the scope of the Data Migration solution, include Storage, API requests, Compaction and Monitoring. 

## Cluster Configurations

Example EMR cluster pricing is based upon the example cluster resources below.

| Cluster Size | Primary Node                | Core Nodes             | Task Nodes             |
|--------------|-----------------------------|-----------------------|-----------------------|
| Xlarge       | 1× r5.4xlarge               | 8× i3.4xlarge         | 12× i3.4xlarge        |
| Large        | 1× r5.4xlarge               | 4× i3.4xlarge         | 8× i3.4xlarge         |
| Medium       | 1× m5.4xlarge               | 4× i3.4xlarge         | 4× i3.4xlarge         |
| Small        | 1× m5.4xlarge               | 1× i3.4xlarge         | 1× i3.4xlarge         |

## Hourly Rates

### EC2 Instance Prices
- r5.4xlarge: $1.008/hr
- i3.4xlarge: $1.248/hr
- m5.4xlarge: $0.768/hr

### EMR Fees per Instance Type
- r5.4xlarge: $0.252/hr
- i3.4xlarge: $0.27/hr
- m5.4xlarge: $0.192/hr

## Additional Services
- CloudWatch Logs: $0.50 per GB ingested
- Step Functions: $0.000025 per state transition
- SNS: $0.50 per 1 million requests
- S3 Requests:
  - HEAD: $0.005 per 1,000 requests
  - GET: $0.0004 per 1,000 requests

Note: All prices are rounded to the nearest cent for the final totals.
