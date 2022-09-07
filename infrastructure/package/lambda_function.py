import boto3
from os import environ


def handler(event, context):
    """
    Lambda function that starts a job flow in EMR.
    """
    client = boto3.client('emr', region_name='us-east-1')

    cluster_id = client.run_job_flow(
        Name=environ['CLUSTER_NAME'],
        ServiceRole=environ['SERVICE_ROLE'],
        JobFlowRole=environ['JOB_FLOW_ROLE'],
        AutoScalingRole=environ['AUTO_SCALING_ROLE'],
        VisibleToAllUsers=True,
        LogUri=environ['LOGS_URI'],
        ReleaseLabel='emr-6.7.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                },
                {
                    'Name': 'Worker nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                }
            ],
            'Ec2KeyName': 'execute-emr-key',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-005e758940508dc63'
        },

        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hive'},
            {'Name': 'Pig'},
            {'Name': 'Hue'},
            {'Name': 'JupyterHub'},
            {'Name': 'JupyterEnterpriseGateway'},
            {'Name': 'Livy'},
        ],

        Configurations=[{
            "Classification": "spark-env",
            "Properties": {},
            "Configurations": [{
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                        }]
        },
            {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        },
            {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.submit.deployMode": "cluster",
                "spark.speculation": "false",
                "spark.sql.adaptive.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            }
        },
            {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }
        ],

        StepConcurrencyLevel=1,

        Steps=[{
            'Name': 'Delta Insert do ENEM',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--packages', 'io.delta:delta-core_2.12:1.0.0',
                                 '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
                                 '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 environ['DATA_INSERT_KEY']
                                 ]
                    }
        },
            {
            'Name': 'Simulacao e UPSERT do ENEM',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--packages', 'io.delta:delta-core_2.12:1.0.0',
                                 '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
                                 '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 environ['DATA_UPSERT_KEY']
                                 ]
                    }
        }],
    )

    return {
        'statusCode': 200,
        'body': f"Started job flow {cluster_id['JobFlowId']}"
    }
