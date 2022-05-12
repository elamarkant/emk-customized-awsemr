# Report EMR Spot Instance interruption
# Run in Lambda triggerd by cloudwatch events Spot interruption
# Print EMR cluster id and Instance Group type to notification
# Ver 1.0

import json
import boto3

def lambda_handler(event, context):
    # get instance id and state
    instanceId = event['detail']['instance-id']
    instanceAction = event['detail']['instance-action']
    # post spot interruption 
    if instanceAction != "terminate":
        print("It's not a termination for instance id:", instanceId, "It's action is:", instanceAction)
        return
    else:
        # get loss instance and its information
        try:
            ec2_client = boto3.client('ec2', region_name = 'cn-north-1')
            ec2_response = ec2_client.describe_instances(
                Filters=[
                    {
                        'Name': 'tag-key',
                        'Values': ['aws:elasticmapreduce:job-flow-id']
                    }
                ],
                InstanceIds=[instanceId]
            )
        except:
            return
        # prepare notification message, if it is an EMR instance
        try:
            instanceType = ec2_response['Reservations'][0]['Instances'][0]['InstanceType']
            instanceTags = ec2_response['Reservations'][0]['Instances'][0]['Tags']
        except:
            print("It's not a EMR instance, instance id is ", instanceId)
            return
        # extract cluster id and instance group from instance tags
        for tags in instanceTags:
            if tags['Key'] == 'aws:elasticmapreduce:job-flow-id':
                instanceCluster = tags['Value']
            elif tags['Key'] == 'aws:elasticmapreduce:instance-group-role':
                instanceRole = tags['Value']
        sns_message = {"instance Id": instanceId,"instance Type": instanceType,"EMR Cluster Id": instanceCluster,"EMR Instance Groups":instanceRole}
        # send sns message
        try:
            sns_client = boto3.client('sns')
            sns_response = sns_client.publish(
                TargetArn = "arn:aws-cn:sns:cn-north-1:354530833153:s3-restore",
                Subject = "Spot loss of EMR",
                Message = json.dumps(sns_message)
            )
        except:
            print("Send Message Error")
            return
        return