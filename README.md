# Guidance for Migrating Apache Iceberg Data from Amazon S3 to S3 Tables

## Table of Contents
1. [Introduction](#introduction)
2. [Core Services](#core-services)
3. [Solution Overview](#solution-overview)
4. [Costs](#costs)
5. [Deployment options](#deployment-options)
6. [Deployment steps](#deployment-steps)
7. [Post deployment steps](#post-deployment-steps)
8. [Cleanup](#cleanup) 
9. [Customer responsibility](#customer-responsibility)
10. [Feedback](#feedback)
12. [Notices](#notices)

---

<a name="introduction"></a>
## Introduction

This user guide is created for anyone who is interested in Migrating Apache Iceberg Data to [Amazon S3 Tables](https://aws.amazon.com/s3/features/tables/). This solution sets up an automated migration solution for moving data from an existing [Amazon S3](https://aws.amazon.com/s3/) bucket and [AWS Glue Table](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html) to an Amazon S3 Tables Bucket using [AWS Step Functions](https://aws.amazon.com/step-functions/) and [Amazon EMR](https://aws.amazon.com/emr/) with [Apache Spark](https://spark.apache.org/). 

<a name="core-services"></a>
## Core Services
Below is a high-level overview of the Core Services that are incorporated into Migrating Apache Iceberg Data from Amazon S3 to S3 Tables. We will assume the reader is familiar with Git, Python, and AWS.

| Service | Description |
|---------|-------------|
| [AWS CloudFormation](https://aws.amazon.com/cloudformation/) | Speed up cloud provisioning with infrastructure as code. |
| [Amazon S3](https://aws.amazon.com/s3/) | An object storage service that offers industry-leading scalability, data availability, security, and performance. |
| [Amazon S3 Tables](https://aws.amazon.com/s3/features/tables/) | Delivering the first cloud object store with built-in Apache Iceberg support and streamline storing tabular data at scale. |
| [AWS Lambda](https://aws.amazon.com/lambda/) | Serverless compute service that runs code in response to events and automatically manages the compute resources, enabling developers to build applications that scale with business needs. |
| [AWS Step Functions](https://aws.amazon.com/step-functions/) | Create workflows to build distributed applications, automate processes, orchestrate microservices, and create data and machine learning pipelines. |
| [Amazon Simple Notification Service](https://aws.amazon.com/pm/sns) | A managed service that provides message delivery from publishers to subscribers. |
| [Amazon Identity and Access Management](https://aws.amazon.com/iam) | Securely manage identities and access to AWS services and resources. |

---

<a name="solution-overview"></a>
## Solution Overview

This solution deploys an AWS Step Functions state machine to manage the migration workflow along with several other supporting AWS resources (AWS IAM roles, Amazon SNS topic, and an Amazon S3 bucket for EMR logs). In addition to the standard AWS resources, a [CloudFormation Custom Resource](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/template-custom-resources.html), via AWS Lambda, is used to validate the existence of the source S3 Bucket and AWS Glue Table. Once deployed, the user manually starts the EMREC2StateMachine workflow which deploys an EMR cluster, starts an Apache Spark job execution on the deployed cluster, and finally deletes the cluster once the migration is completed. The Apache Spark job uses CTAS (Create Table as Select) to migrate the Iceberg data from the source S3 Bucket to the target S3 Tables Bucket. Throughout the migration process, the Step Function sends status notifications via SNS to the user.

Upon completion, the Step Function send an email via SNS to the user that the workflow has completed, and the EMR Cluster is then terminated by the EMREC2StateMachine Step Function task.

![Architecture Diagram](./images/ra.png) 

<p align="center">
<em>Figure 1. Migrating Apache Iceberg Data from Amazon S3 to S3 Tables Reference Architecture.</em>
</p>

<p align="center">
<img src="./images/s3tables-migration-workflow.png" alt="AWS Step Functions Flow Chart">
</p>

<p align="center">
<em>Figure 2. AWS Step Functions Flow Chart.</em>
</p>

<a name="costs"></a>
## Costs
## Costs and Licenses
While utilizing this guidance sample code doesn't incur any direct charges, please be aware that you will incur costs for the AWS services or resources activated by this guidance architecture. The cost will vary based upon the amount of data that requires migration, number of objects, and the size of the EMR cluster creation.

In this cost example, we examine the cost over a month for a 1TiB S3 Bucket migration with updates creating 1,000 new data files with an average object size of 5 MB and 3 metadata files with an average object size of 10 KB. The table users frequently perform queries on the data-set and generate 500,000 GET requests per month. To optimize query performance, automatic compaction is enabled. In addition we are including the cost of a Small EMR Cluster option, detailed below in deployment costs, for the duration of the migration.

### Example Costs

You are responsible for the cost of the AWS services used while running this solution. As of December 2024, the cost for running this solution with the small Implementation of EMR in US West-2 (US Oregon) Region is approximately **\$ 45.68 per month**, assuming **1 TiB of Data and 1,000 Objects are migrated**.

This guidance uses [Serverless services](https://aws.amazon.com/serverless/), which use a on-demand billing model - costs are incurred with usage of the deployed resources. Refer to the [Sample cost table](#sample-cost-table) for a service-by-service cost breakdown.

We recommend creating a [budget](https://alpha-docs-aws.amazon.com/awsaccountbilling/latest/aboutv2/budgets-create.html) through [AWS Cost Explorer](http://aws.amazon.com/aws-cost-management/aws-cost-explorer/) to help manage costs. Prices are subject to change. For full details, refer to the pricing webpage for each AWS service used in this guidance.

#### Sample cost table

The following table provides a sample cost breakdown for deploying this guidance with the default parameters in the `us-east-2` (US Oregon) Region for one month assuming "non-production" level volume of uploaded files.

| Cost Component                      | Calculation                                                | Cost     |
|----------------------------------------|------------------------------------------------------------|---------:|
| Free Tier Eligible | | | 
| - AWS Step Functions                      | 7 state transitions                    | $0.00    |
| - Amazon SNS                                 | 5 notifications                        | $0.00    |
| - AWS Lambda                              | Custom resource functions              | $0.00    |
| - Amazon IAM                                 | IAM roles and policies                               | $0.00    |
| Amazon EMR Cluster (3 hours)               |                                                            |          |
| - m6gd.4xlarge (Primary)            | $0.9664/hour * 3 hours                                     | $2.87    |
| - i3.4xlarge (Core)                 | $1.248/hour * 3 hours                                      | $3.74    |
| - i3.4xlarge (Task)                 | $1.248/hour * 3 hours                                      | $3.74    |
| - EMR Service Fee                     | $0.015/hour/instance * 3 instances * 3 hours               | $0.14    |
| Amazon S3 | | | 
| - S3 Tables Storage (Monthly)         | 1,024 GB * $0.0265/GB                                      | $27.14   |
| - S3 Tables PUT Requests              | 30,090 requests * $0.005/1,000 requests                    | $0.15    |
| - S3 Tables GET Requests              | 500,000 requests * $0.0004/1,000 requests                  | $0.20    |
| - S3 Tables Object Monitoring         | 10,486 objects * $0.025/1,000 objects                      | $0.26    |
| - S3 Tables Compaction (Objects)      | 30,000 objects * $0.004/1,000 objects                      | $0.12    |
| - S3 Tables Compaction (Data)         | 146.48 GB * $0.05/GB                                       | $7.32    |
| **Total Cost**                      |                                                            | **$45.68**|

### Assumptions and Notes:
- EMR Cluster runs for 3 hours for the migration job
- EC2 instance prices are based on US West-2 (US Oregon) region, On-Demand pricing
- EMR pricing includes EC2 instance cost plus a service fee
- S3 Tables costs are calculated on a monthly basis as per the previous scenario
- The total cost includes both the one-time migration cost (EMR) and the first month of S3 Tables usage
- Actual costs may vary based on region, specific data patterns, and any additional AWS services used

<a name="deployment-options"></a>

## Deployment Options

- Click on the following link for a Console Launch via  [![Cloud Formation Template](./images/cf_button.png)]() **Need link @tjm
- Launch via [AWS CloudShell](https://aws.amazon.com/cloudshell/)
---

Provide below is the list of EMR Clusters that are deployable from the CloudFormation template. This decision is based off of the organization requirements.

### EMR Cluster Sizes

| Size   | Primary Instance            | Core Instances              | Task Instances              |
|--------|-----------------------------|-----------------------------|----------------------------|
| Small  | 1 x m6gd.4xlarge            | 1 x i3.4xlarge              | 1 x i3.4xlarge              |
| Medium | 1 x m5.4xlarge              | 4 x i3.4xlarge              | 4 x i3.4xlarge              |
| Large  | 3 x r5.4xlarge              | 4 x i3.4xlarge              | 8 x i3.4xlarge              |
| XLarge | 3 x r5.4xlarge              | 8 x i3.4xlarge              | 12 x i3.4xlarge             |

### Cluster Performance Configuration

| Size   | Executor Memory | Executor Cores | Driver Memory | Driver Cores | Min Executors | Max Executors |
|--------|-----------------|----------------|---------------|--------------|---------------|---------------|
| Small  | 24G             | 4              | 24G           | 4            | 2             | 7             |
| Medium | 24G             | 4              | 24G           | 4            | 8             | 29            |
| Large  | 24G             | 3              | 32G           | 4            | 12            | 44            |
| XLarge | 28G             | 4              | 48G           | 4            | 20            | 74            |


<a name="deployment-steps"></a>

## Deployment Steps

### AWS CloudShell Deployment Steps

** Need updated gitrepo - @tjm

1. **Clone sample code GitHub repository using the following command:**
```
$ git clone https://github.com/aws-solutions-library-samples/guidance-for-migrating-apache-iceberg-data-from-s3-to-s3-tables.git
Cloning into 'guidance-for-migrating-apache-iceberg-data-from-s3-to-s3-tables'...
remote: Enumerating objects: 29, done.
remote: Counting objects: 100% (29/29), done.
remote: Compressing objects: 100% (20/20), done.
remote: Total 29 (delta 10), reused 26 (delta 9), pack-reused 0 (from 0)
Receiving objects: 100% (29/29), 14.22 KiB | 2.84 MiB/s, done.
Resolving deltas: 100% (10/10), done.
```

2. **Change directory to the src subdirectory located at 'guidance-for-migrating-apache-iceberg-data-from-s3-to-s3-tables/src':**
```
cd guidance-for-migrating-apache-iceberg-data-from-s3-to-s3-tables/src
```

3. **Define the following Parameters for CloudFormation**

### Source Data
| Parameter | Description |
|-----------|-------------|
| `YourS3Bucket` | Source S3 bucket containing the data to migrate |
| `YourExistingGlueDatabase` | Source Glue database name |
| `YourExistingGlueTable` | Source Glue table name |

### Source Partitioning (Optional)
| Parameter | Description |
|-----------|-------------|
| `YourExistingGlueTablePartitionName` | Source table partition name |
| `YourExistingGlueTablePartitionValue` | Source table partition value |

### Destination
| Parameter | Description |
|-----------|-------------|
| `S3TableBucket` | Destination S3 Tables Bucket ARN |
| `S3TableBucketNamespace` | Destination S3 Tables namespace/database |
| `S3TableBucketTables` | Destination S3 Tables table name |

> **Note**: Ensure all required parameters are correctly set before initiating the migration process.

---
4. **Launch CloudFormation template from CLI:**
```
aws cloudformation create-stack --stack-name s3TablesMigration --template-body file://automated-migration-to-s3-tables-latest.yaml --capabilities CAPABILITY_NAMED_IAM
```
---

## Post Deployment Steps

### 1. Confirm SNS Subscription
- Remember to confirm the SNS subscription to the e-mail address selected.

### 2. Upload PySpark Script
- Go to Stack Resources and locate the solution EMR log Bucket
- Upload the PySpark script from the [scripts](scripts/pyspark/mys3tablespysparkscript.py) repo to:

> resources/scripts/mys3tablespysparkscript.py

### 3. Start Step Function Execution
- In Stack Resources, find and click on "EMREC2StateMachine" link
- At the Step Function summary section:
1. Click "Start Execution"
2. Enter `{}` as the input
3. Begin the workflow

### 4. Monitor the Process
Choose one of these methods to monitor:
- Step Function Console
- EMR Management Console

> **Tip:** Keep an eye on the SNS notifications for important updates about the migration process.

---
<a name="Cleanup"></a>
## Cleanup
In order to un-deploy the guidance code from your AWS account, the following steps have to be made:

1. Through the AWS Manager console, you can navigate to CloudFormation in the console, choose the stack as named at deployment, and choose Delete.

2. Though the AWS Console execute the following command:
```
aws cloudformation delete-stack --stack-name s3TablesMigration
```
Or if you have named the stack something else, replace "s3TablesMigration" with that stack name.

**Once deleted, the resources associated with the migration solution are no longer available, but the S3 Tables data and the EMR Logs S3 bucket are retained.**

---

<a name="customer-responsibility"></a>
## Customer Responsibility
Upon deploying the guidance, ensure that all your resources and services are up-to-date and appropriately configured. This includes the application of necessary patches to align with your security requirements and other specifications. For a comprehensive understanding of the roles and responsibilities in maintaining security, please consult the [AWS Shared Responsibility Model](https://aws.amazon.com/compliance/shared-responsibility-model).

---
<a name="feedback"></a>
## Feedback

To submit feature ideas and report bugs, use the [Issues section of the GitHub repository](https://github.com/aws-solutions-library-samples/guidance-for-migrating-apache-iceberg-data-from-s3-to-s3-tables/issues) for this guidance.

---
<a name="notices"></a>
## Notices

This document is provided for informational purposes only. It represents current AWS product offerings and practices as of the date of issue of this document, which are subject to change without notice. Customers are responsible for making their own independent assessment of the information in this document and any use of AWS products or services, each of which is provided "as is" without warranty of any kind, whether expressed or implied. This document does not create any warranties, representations, contractual commitments, conditions, or assurances from AWS, its affiliates, suppliers, or licensors. The responsibilities and liabilities of AWS to its customers are controlled by AWS agreements, and this document is not part of, nor does it modify, any agreement between AWS and its customers.

The software included with this guidance is licensed under the MIT License, version 2.0 (the "License"). ermission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

---
