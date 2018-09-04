import os
from argparse import ArgumentParser
import json
from argparse import RawTextHelpFormatter

release_label = 'emr-5.14.0'

### These are for beta site setting
# KeyName = 'EricBVTwest'
# SubnetId = 'subnet-1ea1c745'
# EmrManagedMasterSecurityGroup = 'sg-c818c8b5'
# EmrManagedSlaveSecurityGroup = 'sg-4717c73a'

### These are for production site setting
KeyName = 'production-eric'
SubnetId = 'subnet-6118a129'
EmrManagedMasterSecurityGroup = 'sg-b038a2cd'
EmrManagedSlaveSecurityGroup = 'sg-ba3fa5c7'


def new_deploy(args):
    response = os.popen('aws emr list-clusters --active --profile {0}'.format(args.profile)).read()
    try:
        list_cluster_dict = json.loads(response)
    except:
        print(response)
        exit()
    name_exist = False
    for cluster in list_cluster_dict['Clusters']:
        if cluster['Name'] == 'Data Pipeline 2 {0} Site'.format(
                args.beta_or_production.capitalize()) and name_exist == False:
            cluster_id = cluster['Id']
            name_exist = True
    if name_exist:
        print('Cluster with name "Data Pipeline 2 {0} Site" has already existed, aborting...'.format(
            args.beta_or_production.capitalize()))
        exit()
    if args.beta_or_production.lower() == 'beta':
        beta_command = 'aws emr create-cluster --profile %s --termination-protected --applications Name=Hadoop Name=Hive Name=Hue Name=Tez Name=Oozie Name=Spark --ec2-attributes \'{"KeyName":"%s","InstanceProfile":"ADFS-OPS","SubnetId":"%s","EmrManagedSlaveSecurityGroup":"%s","EmrManagedMasterSecurityGroup":"%s"}\' --release-label %s --log-uri \'s3n://trs-production-logs-us-west-2/\' --steps \'[{"Args":["s3://trs-production-emr-us-west-2/bootstrap/emr-step.sh"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar","Properties":"","Name":"EMR init"},{"Args":["s3://trs-production-emr-us-west-2/deploy_tool_step/beta_new_deploy.sh"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar","Properties":"","Name":"Beta Site New Deploy"}]\' --instance-groups \'[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"CORE","InstanceType":"r4.xlarge","Name":"Core - 2"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"c5.4xlarge","Name":"Master - 1"}]\' --configurations \'[{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"},"Configurations":[]},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"},"Configurations":[]}]\' --auto-scaling-role EMR_AutoScaling_DefaultRole --ebs-root-volume-size 10 --service-role ADFS-OPS --enable-debugging --name \'Data Pipeline 2 Beta Site\' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-west-2' % (
        args.profile, KeyName, SubnetId, EmrManagedSlaveSecurityGroup, EmrManagedMasterSecurityGroup, release_label)
        os.system(beta_command)

    elif args.beta_or_production.lower() == 'production':
        production_command = 'aws emr create-cluster --profile %s --termination-protected --applications Name=Hadoop Name=Hive Name=Hue Name=Tez Name=Oozie Name=Spark --ec2-attributes \'{"KeyName":"%s","InstanceProfile":"ADFS-OPS","SubnetId":"%s","EmrManagedSlaveSecurityGroup":"%s","EmrManagedMasterSecurityGroup":"%s"}\' --release-label %s --log-uri \'s3n://trs-production-logs-us-west-2/\' --steps \'[{"Args":["s3://trs-production-emr-us-west-2/bootstrap/emr-step.sh"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar","Properties":"","Name":"EMR init"},{"Args":["s3://trs-production-emr-us-west-2/deploy_tool_step/production_new_deploy.sh"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar","Properties":"","Name":"Production Site New Deploy"}]\' --instance-groups \'[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"c5.4xlarge","Name":"Master - 1"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":300,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"CORE","InstanceType":"r4.4xlarge","Name":"Core - 2"},{"InstanceCount":4,"BidPrice":"OnDemandPrice","AutoScalingPolicy":{"Constraints":{"MinCapacity":1,"MaxCapacity":4},"Rules":[{"Action":{"SimpleScalingPolicyConfiguration":{"ScalingAdjustment":1,"CoolDown":0,"AdjustmentType":"CHANGE_IN_CAPACITY"}},"Description":"","Trigger":{"CloudWatchAlarmDefinition":{"MetricName":"MemoryTotalMB","ComparisonOperator":"LESS_THAN","Statistic":"AVERAGE","Period":300,"Dimensions":[{"Value":"${emr.clusterId}","Key":"JobFlowId"}],"EvaluationPeriods":1,"Unit":"COUNT","Namespace":"AWS/ElasticMapReduce","Threshold":1100000}},"Name":"Keep spot instance as 4"}]},"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":200,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"TASK","InstanceType":"r4.8xlarge","Name":"Task - 3"}]\' --configurations \'[{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"},"Configurations":[]},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"},"Configurations":[]}]\' --auto-scaling-role EMR_AutoScaling_DefaultRole --ebs-root-volume-size 10 --service-role ADFS-OPS --enable-debugging --name \'Data Pipeline 2 Production Site\' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-west-2' % (
        args.profile, KeyName, SubnetId, EmrManagedSlaveSecurityGroup, EmrManagedMasterSecurityGroup, release_label)

        os.system(production_command)


def change_build(args):
    response = os.popen('aws emr list-clusters --active --profile {0}'.format(args.profile)).read()
    try:
        list_cluster_dict = json.loads(response)
    except:
        print(response)
        exit()
    name_exist = False
    for cluster in list_cluster_dict['Clusters']:
        if cluster['Name'] == 'Data Pipeline 2 {0} Site'.format(
                args.beta_or_production.capitalize()) and name_exist == False:
            cluster_id = cluster['Id']
            name_exist = True
        elif cluster['Name'] == 'Data Pipeline {0} Site'.format(
                args.beta_or_production.capitalize()) and name_exist == True:
            print('Multiple clusters have same name "Data Pipeline 2 {0} Site", aborting...'.format(
                args.beta_or_production.capitalize()))
            exit()
    if name_exist == False:
        print('Cluster not found')
        exit()
    os.system(
        "aws emr add-steps --profile {1} --cluster-id {0} --steps Type=CUSTOM_JAR,Name='{2} Site Change Build',ActionOnFailure=CONTINUE,Jar=s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://trs-production-emr-us-west-2/deploy_tool_step/{3}_change_build.sh".format(
            cluster_id, args.profile, args.beta_or_production.capitalize(), args.beta_or_production.lower()))


def resize_instance(args):
    response = os.popen('aws emr list-clusters --active --profile {0}'.format(args.profile)).read()
    try:
        list_cluster_dict = json.loads(response)
    except:
        print(response)
        exit()
    name_exist = False
    for cluster in list_cluster_dict['Clusters']:
        if cluster['Name'] == 'Data Pipeline 2 {0} Site'.format(
                args.beta_or_production.capitalize()) and name_exist == False:
            cluster_id = cluster['Id']
            name_exist = True
        elif cluster['Name'] == 'Data Pipeline 2 {0} Site'.format(
                args.beta_or_production.capitalize()) and name_exist == True:
            print('Multiple clusters have same name "Data Pipeline 2 {0} Site", aborting...'.format(
                args.beta_or_production.capitalize()))
            exit()
    if name_exist == False:
        print('Cluster not found')
        exit()
    instance_dict = json.loads(os.popen(
        'aws emr list-instances --cluster-id {0} --instance-group-types CORE --profile {1}'.format(cluster_id,
                                                                                                   args.profile)).read())
    instance_group_id = instance_dict['Instances'][0]['InstanceGroupId']
    os.system(
        'aws emr modify-instance-groups --instance-groups InstanceGroupId={0},InstanceCount={1} --profile {2}'.format(
            instance_group_id, args.resize_num, args.profile))


def terminate(args):
    response = os.popen('aws emr list-clusters --active --profile {0}'.format(args.profile)).read()
    try:
        list_cluster_dict = json.loads(response)
    except:
        print(response)
        exit()
    name_exist = False
    for cluster in list_cluster_dict['Clusters']:
        if cluster['Name'] == 'Data Pipeline 2 {0} Site'.format(
                args.beta_or_production.capitalize()) and name_exist == False:
            cluster_id = cluster['Id']
            name_exist = True
        elif cluster['Name'] == 'Data Pipeline 2 {0} Site'.format(
                args.beta_or_production.capitalize()) and name_exist == True:
            print('Multiple clusters have same name "Data Pipeline 2 {0} Site", aborting...'.format(
                args.beta_or_production.capitalize()))
            exit()
    if name_exist == False:
        print('cluster name not found...')
        exit(-1)

    cluster_id_by_user = raw_input(
        'Please retype the cluster ID to confirm termination(cluster ID={0}): '.format(cluster_id))
    if cluster_id_by_user != cluster_id:
        print('cluster ID mismatch, aborting...')
        exit()
    else:
        if args.force:
            os.system(
                'aws emr modify-cluster-attributes --cluster-id {0} --no-termination-protected --profile {1}'.format(
                    cluster_id, args.profile))
        os.system('aws emr terminate-clusters --cluster-id {0} --profile {1}'.format(cluster_id, args.profile))
        return


if __name__ == '__main__':
    parser = ArgumentParser(epilog='examples:\n\
    python aws_cluster_action.py <profile> new_deploy beta\n\
    python aws_cluster_action.py <profile> change_build beta\n\
    python aws_cluster_action.py <profile> resize_instance beta 2\n\
    python aws_cluster_action.py <profile> terminate beta [-f]\n', formatter_class=RawTextHelpFormatter)
    parser.add_argument('profile', type=str, help='profile for credentials')
    subparsers = parser.add_subparsers(title='operations', description='aws operation', help='aws operation you need',
                                       dest='action')

    parser_new_deploy = subparsers.add_parser('new_deploy')
    parser_new_deploy.add_argument('beta_or_production', type=str, help='beta or production',
                                   choices=['beta', 'production'])

    parser_change_build = subparsers.add_parser('change_build')
    parser_change_build.add_argument('beta_or_production', type=str, help='beta or production build file',
                                     choices=['beta', 'production'])

    parser_resize_instance = subparsers.add_parser('resize_instance')
    parser_resize_instance.add_argument('beta_or_production', type=str, help='beta or production build file',
                                        choices=['beta', 'production'])
    parser_resize_instance.add_argument('resize_num', type=int, help='target size of cores')

    parser_terminate = subparsers.add_parser('terminate')
    parser_terminate.add_argument('beta_or_production', type=str, help='beta or production build file',
                                  choices=['beta', 'production'])
    parser_terminate.add_argument('-f', '--force', help='remove termination protection', action='store_true')

    args = parser.parse_args()

    if (args.action == 'new_deploy'):
        new_deploy(args)
    elif (args.action == 'change_build'):
        change_build(args)
    elif (args.action == 'resize_instance'):
        resize_instance(args)
    elif (args.action == 'terminate'):
        terminate(args)
    else:
        print(args.action + 'action not supported')
