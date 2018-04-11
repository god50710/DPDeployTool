import os
import re
import sys
import subprocess
from datetime import datetime
from optparse import OptionParser

''' Detect current environment '''
if re.search('trs-hive01-', os.environ['HOSTNAME']):
    DEFAULT_PATH = "../../output/data-pipeline-parquet/op-utils/"
else:
    DEFAULT_PATH = "../../output/data-pipeline-aws/op-utils"

JOB_NEEDS_SELF_SUCCESS_FLAG = ["f_router_collection_weekly", "f_device_collection_daily",
                               "f_ips_hit_rule_collection_daily", "f_cam_collection_weekly", "f_cam_collection_daily",
                               "f_cam_ips_hit_rule_collection_daily"]


def run_command(cmd, show_command=True):
    if show_command:
        print(cmd)
    o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = o.communicate()
    if result[1] != "":
        print(result)
    else:
        print(result[0])


def replace_new_time(job, app_info_list):
    if OPS.dryrun:
        print('=== Replace new deploy time ===')
    else:
        print('=== Preview new deploy time ===')
    id_endtime = app_info_list[job]
    start = '%s:\tcoordStart=%sZ' % (job, re.sub(' ', 'T', id_endtime[1].rstrip(' ')))
    if OPS.dryrun:
        run_command('sed -i "s/%s:\tcoordStart.*/%s/g" %s/app-time.conf' % (job, start, DEFAULT_PATH))
    else:
        print(start)

    end = '%s:\tcoordEnd=2117-10-01T00:00Z' % job
    if OPS.dryrun:
        run_command('sed -i "s/%s:\tcoordEnd.*/%s/g" %s/app-time.conf' % (job, end, DEFAULT_PATH))
    else:
        print(end)


def dryrun_job(job, app_info_list):
    if re.match('T', job) or re.match('S', job):
        for each in app_info_list:
            replace_new_time(each, app_info_list)

        job_string = ''
        for job in app_info_list:
            job_string = job_string + job + ' '

        print('=== Dry-run job ===')
        run_command("cd %s;./deploy.sh %s" % (DEFAULT_PATH, job_string))
    else:
        replace_new_time(job, app_info_list)

        print('=== Dry-run job ===')
        run_command("cd %s;./deploy.sh %s" % (DEFAULT_PATH, job))


def kill_job(job, app_info_list):
    print('=== Kill Job ===')
    run_command("oozie job -kill %s" % app_info_list[job][0])


def run_job(job, app_info_list):
    if re.match('T', job or re.match('S', job)):
        for each in app_info_list:
            kill_job(each, app_info_list)
    else:
        kill_job(job, app_info_list)

    print('=== Run Job ===')
    run_command("cd %s/;./run-jobs.sh" % DEFAULT_PATH)


def check_job(job, app_info_list, quiet_mode=False):
    if quiet_mode:
        hide_success_job = '\|SUCCEEDED'
    else:
        hide_success_job = ''
    if re.match('T', job) or re.match('S', job):
        counter = 1
        for app in app_info_list:
            print('\n=== Job Checking(%d/%d) ===' % (counter, len(app_info_list)))
            run_command("oozie job -info %s |grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" % (
                app_info_list[app][0], hide_success_job), False)
            if not quiet_mode:
                replace_new_time(app, app_info_list)
            counter += 1
    else:
        if job in app_info_list:
            print('=== Job Checking ===')
            run_command("oozie job -info %s |grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" % (
                app_info_list[job][0], hide_success_job), False)
            if not quiet_mode:
                replace_new_time(job, app_info_list)
        else:
            print('Job not found in app_info_list')


def get_app_info(job):
    print('\nCurrent status of Oozie job:')
    cmd = "oozie jobs info -jobtype coordinator -len 500|grep '%s.*RUNNING\|%s.*PREP'|sort -k8" % (job, job)
    # for offline debug
    # cmd = "cat ./temp.log|grep '^0'|grep 'dp'|sort -k8"
    o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    info = o.communicate()[0]
    info = info.rstrip('\n').split('\n')
    print("JobID\t\t\t\t     Next Materialized    App Name")
    app_info = {}
    for each in info:
        result = re.findall('(.*-oozie-oozi-C)[ ]*(%s.*)\.[\S ]*RUNNING.*GMT    ([0-9: -]*).*    ' % job, each)
        if len(result) > 0:
            print
            result[0][0], result[0][2], result[0][1]
            app_info.update({result[0][1]: [result[0][0], result[0][2]]})
    print('\nCurrent time: %s' % datetime.now())
    print()
    return app_info


def get_s3_path():
    '''obtain S3 path(ex. HADOOP_NAME_NODE=s3://trs-data-pipeline-us-west-2/Eric/dp_eric)'''
    cmd = 'cat %s/env/hadoop@%s|grep -o "s3://.*"' % (DEFAULT_PATH, os.environ['HOSTNAME'])
    o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return o.communicate()[0]


def add_flag(job):
    '''create _SUCCESS flag for initial job status'''
    print('Start creating flag.......')
    '''obtain Date which needs _SUCCESS flag'''
    cmd = 'cat %s/app-time.conf|grep "T2DeviceCollection.*Start"|grep -o "20[0-9\-]*"' % DEFAULT_PATH
    o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    date_t2 = o.communicate()[0].rstrip('\n')
    cmd = 'date -d "%s -2 day" ' % date_t2 + '+%Y-%m-%d'
    o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    date = o.communicate()[0].rstrip('\n')

    if re.search('trs-hive01-', os.environ['HOSTNAME']):
        s3_path = ""
        job = "broadweb"
    else:
        s3_path = get_s3_path()
        job = "dp"

    for each in JOB_NEEDS_SELF_SUCCESS_FLAG:
        cmd = "hadoop fs -mkdir -p %s/Application/shnprj_spn/hive/%s.db/%s/d=%s/;" % (s3_path, job, each, date)
        print(cmd)
        o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = o.communicate()[0]
    exit()


def kill_flag(job):
    '''obtain Date which needs _SUCCESS flag'''
    cmd = 'cat %s/app-time.conf|grep "T0RouterInfo.*Start"|grep -o "20[0-9\-]*"' % DEFAULT_PATH
    o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    date_start = o.communicate()[0].rstrip('\n')

    if re.search('trs-hive01-', os.environ['HOSTNAME']):
        s3_path = ""
        job = "broadweb"
    else:
        s3_path = get_s3_path()
        job = "dp"

    print("Please refer to below commands and execute with what you need:")
    print("For one day:")
    print("hadoop fs -rm %s/Application/shnprj_spn/hive/%s.db/*/d=%s/_SUCCESS" % (s3_path, job, date_start))
    print("hadoop fs -rm %s/Application/shnprj_spn/hive/%s.db/*/d=%s/h=*/_SUCCESS" % (s3_path, job, date_start))
    print("For a period:")
    print("hadoop fs -rm %s/Application/shnprj_spn/hive/%s.db/*/d=2017-11-*/_SUCCESS" % (s3_path, job))
    print("hadoop fs -rm %s/Application/shnprj_spn/hive/%s.db/*/d=2017-11-*/h=*/_SUCCESS" % (s3_path, job))
    exit()


def command_parser():
    '''command parser for common usage'''
    usage = "\t%s [options]\nTool version:\t%s" % (sys.argv[0], "20180222")
    parser = OptionParser(usage)
    parser.add_option("-j", type="string", dest="job",
                      help='choose one job(ex. "T1Device") or "T" for all jobs. This option must company with option -c/-r.')
    parser.add_option("-c", action="store_true", dest="check", help='check previous job status')
    parser.add_option("-d", action="store_true", dest="dryrun", help='dry-run new job')
    parser.add_option("-r", action="store_true", dest="run",
                      help='kill old job and run new one(make sure already has dry-run first)')
    parser.add_option("-f", action="store_true", dest="clean_flag",
                      help='kill old job and run new one(make sure already has dry-run first)')
    parser.add_option("-a", action="store_true", dest="add_flag",
                      help='add initial _SUCCESS flag for %s based on %s/app-time.conf' % (
                          JOB_NEEDS_SELF_SUCCESS_FLAG, DEFAULT_PATH))
    parser.add_option("-k", action="store_true", dest="kill_flag", help='show command of killing all _SUCCESS flag')
    parser.add_option("-q", action="store_true", dest="quiet", help='verify jobs with quiet mode')

    if len(sys.argv) == 1:
        parser.print_help()
        print('\nQuick Start:')
        print('# To check jobs')
        print('python %s -j T -c -s' % os.path.basename(__file__))
        print('\n# To dryrun jobs')
        print('python %s -j T -d' % os.path.basename(__file__))
        print('\n# Run/Deploy jobs')
        print('python %s -j T -r' % os.path.basename(__file__))
        print('\nOther useful commands:')
        print('oozie jobs info -jobtype=coordinator -len 500')
        print('oozie job -change [ID] -value concurrency=3')
        exit()
    return parser.parse_args()[0]


if __name__ == "__main__":
    OPS = command_parser()
    if not OPS.job:
        print('Please choose at least one job to work. Ex. -j T1Device for one job or "-j T" for all data jobs')
    else:
        if OPS.check:
            if OPS.quiet:
                check_job(OPS.job, get_app_info(OPS.job), OPS.quiet)
            else:
                check_job(OPS.job, get_app_info(OPS.job))
        elif OPS.dryrun:
            dryrun_job(OPS.job, get_app_info(OPS.job))
        elif OPS.run:
            run_job(OPS.job, get_app_info(OPS.job))
        elif OPS.add_flag:
            add_flag(OPS.job)
        elif OPS.kill_flag:
            kill_flag(OPS.job)
        elif OPS.job:
            get_app_info(OPS.job)
        else:
            print('Please at least choose one option for check(-c)/dryrun(-d)/run(-r)/... job.')
