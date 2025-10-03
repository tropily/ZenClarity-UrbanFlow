#!/bin/bash

# This script creates an EMR cluster for processing NYC taxi data.

echo "Starting EMR cluster creation..."

aws emr create-cluster \
    --name "emr_nyc_taxi_trip_data_process" \
    --log-uri "s3://teo-nyc-taxi/logs" \
    --release-label "emr-7.7.0" \
    --service-role "arn:aws:iam::667137120741:role/Teo_EMR_DefaultRole" \
    --unhealthy-node-replacement \
    --ec2-attributes '{"InstanceProfile":"Teo_EMR_EC2_InstanceProfile","EmrManagedMasterSecurityGroup":"sg-0176ed9d4670123f9","EmrManagedSlaveSecurityGroup":"sg-088e2ad36ab38e5bf","KeyName":"teoNewec2_key","SubnetIds":["subnet-00284431d8aebdc28"]}' \
    --applications Name=Hadoop Name=Hive Name=Livy Name=Spark \
    --configurations '[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}},{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
    --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"TASK","Name":"Task - 1","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"Primary","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"CORE","Name":"Core","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}}]' \
    --bootstrap-actions '[{"Args":[],"Name":"Install boto3","Path":"s3://teo-nyc-taxi/scripts/install-boto3.sh"}]' \
    --steps '[
        {
            "Name": "StartDbtThriftServer",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "Jar": "command-runner.jar",
            "Args": ["bash", "-c", "sudo /usr/lib/spark/sbin/start-thriftserver.sh"]
        }
        ]'\
    --region "us-east-1"
