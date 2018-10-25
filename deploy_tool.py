import time
import os
import argparse
import re
import subprocess
import sys
from datetime import datetime, timedelta


class DeployTool(object):
    START_TIME = datetime(2018, 8, 1)
    AWS_PROD_SHN_PATH = "s3://dp-shn-us-west-2/dp_shn"
    AWS_PROD_CAM_PATH = "s3://dp-cam-us-west-2/dp_cam"
    AWS_PROD_MISC_PATH = "s3://dp-misc-us-west-2/dp_misc"
    AWS_PROD_SIG_PATH = "s3://dp-sig-us-west-2/dp_sig"
    AWS_BETA_SHN_PATH = "s3://dp-beta-shn-us-west-2/dp_beta_shn"
    AWS_BETA_CAM_PATH = "s3://dp-beta-cam-us-west-2/dp_beta_cam"
    AWS_BETA_SIG_PATH = "s3://dp-beta-sig-us-west-2/dp_beta_sig"
    AWS_VERIFIED_BUILD_PATH = "s3://dp-aws-services-files-production-us-west-2/build"
    AWS_TESTING_BUILD_PATH = "s3://dp-aws-services-files-production-us-west-2/test_build"
    AWS_SIGNATURE_PATH = "s3://dp-aws-services-files-production-us-west-2/signature"
    OP_PATH = "/home/hadoop/op"
    DISPLAY_COUNT = 100
    TOOL_VERSION = "20181025"
    FLAGS = {'dp_shn': {'t_routerinfo_001_hourly': 'f_routerinfo_001_hourly',
                        't_routerstat_001_hourly': 'f_routerstat_001_hourly',
                        't_device_best_recognition_hourly': 'f_device_best_recognition_hourly',
                        't_device_collection_hourly': 'f_device_collection_hourly',
                        't_device_session_stat_hourly': 'f_device_session_stat_hourly',
                        't_device_traffic_stat_hourly': 'f_device_traffic_stat_hourly',
                        't_device_wrs_hits_stat_hourly': 'f_device_wrs_hits_stat_hourly',
                        't_ips_hit_rule_collection_hourly': 'f_ips_hit_rule_collection_hourly',
                        't_router_collection_hourly': 'f_router_collection_hourly',
                        't_routerinfo_normalized_hourly': 'f_routerinfo_normalized_hourly',
                        't_router_security_event_filtered_hourly': 'f_router_security_event_filtered_hourly',
                        't_router_security_event_raw_hourly': 'f_router_security_event_raw_hourly',
                        't_routerstat_normalized_hourly': 'f_routerstat_normalized_hourly',
                        't_rule_hit_stat_hourly': 'f_rule_hit_stat_hourly',
                        't_security_event_filtered_hourly': 'f_security_event_filtered_hourly',
                        't_security_event_raw_hourly': 'f_security_event_raw_hourly'},
             'dp_cam': {'t_tmis_cam_001_hourly': 'f_tmis_cam_001_hourly',
                        't_cam_bfld_stat_hourly': 'f_cam_bfld_stat_hourly',
                        't_cam_collection_hourly': 'f_cam_collection_hourly',
                        't_cam_feedback_normalized_hourly': 'f_cam_feedback_normalized_hourly',
                        't_cam_ips_hit_rule_collection_hourly': 'f_cam_ips_hit_rule_collection_hourly',
                        't_cam_security_event_filtered_hourly': 'f_cam_security_event_filtered_hourly',
                        't_cam_security_event_raw_hourly': 'f_cam_security_event_raw_hourly',
                        't_cam_session_info_hourly': 'f_cam_session_info_hourly',
                        't_cam_trs_stat_hourly': 'f_cam_trs_stat_hourly'},
             'dp_misc': {'t_ncie_001_hourly': 'f_ncie_001_hourly',
                         't_dp2_major_object_counts_daily': 'f_dp2_major_object_counts_daily'},
             'dp_beta_shn': {'t_routerinfo_001_hourly': 'f_routerinfo_001_hourly',
                             't_routerstat_001_hourly': 'f_routerstat_001_hourly',
                             't_device_best_recognition_hourly': 'f_device_best_recognition_hourly',
                             't_device_collection_hourly': 'f_device_collection_hourly',
                             't_device_session_stat_hourly': 'f_device_session_stat_hourly',
                             't_device_traffic_stat_hourly': 'f_device_traffic_stat_hourly',
                             't_device_wrs_hits_stat_hourly': 'f_device_wrs_hits_stat_hourly',
                             't_ips_hit_rule_collection_hourly': 'f_ips_hit_rule_collection_hourly',
                             't_router_collection_hourly': 'f_router_collection_hourly',
                             't_routerinfo_normalized_hourly': 'f_routerinfo_normalized_hourly',
                             't_router_security_event_filtered_hourly': 'f_router_security_event_filtered_hourly',
                             't_router_security_event_raw_hourly': 'f_router_security_event_raw_hourly',
                             't_routerstat_normalized_hourly': 'f_routerstat_normalized_hourly',
                             't_rule_hit_stat_hourly': 'f_rule_hit_stat_hourly',
                             't_security_event_filtered_hourly': 'f_security_event_filtered_hourly',
                             't_security_event_raw_hourly': 'f_security_event_raw_hourly'},
             'dp_beta_cam': {'t_tmis_cam_001_hourly': 'f_tmis_cam_001_hourly',
                             't_cam_bfld_stat_hourly': 'f_cam_bfld_stat_hourly',
                             't_cam_collection_hourly': 'f_cam_collection_hourly',
                             't_cam_feedback_normalized_hourly': 'f_cam_feedback_normalized_hourly',
                             't_cam_ips_hit_rule_collection_hourly': 'f_cam_ips_hit_rule_collection_hourly',
                             't_cam_security_event_filtered_hourly': 'f_cam_security_event_filtered_hourly',
                             't_cam_security_event_raw_hourly': 'f_cam_security_event_raw_hourly',
                             't_cam_session_info_hourly': 'f_cam_session_info_hourly',
                             't_cam_trs_stat_hourly': 'f_cam_trs_stat_hourly'},
             'datalake': {'akamai_rgom': 'Fake',
                          'akamai_web': 'Fake',
                          'akamai_malicious': 'Fake',
                          'iotlog': 'Fake'},
             'trs_src': {'akamai_malicious_20180319': 'Fake',
                         'honeypot_logs': 'Fake',
                         'honeypot_ssh_2': 'Fake',
                         'honeypot_telnet_2': 'Fake',
                         'tdts_logs': 'Fake'
                         }
             }

    @staticmethod
    def run_command(command, show_command=True, throw_error=True):
        # cmd(string) : command we want to execute on shell
        # show_command(boolean) : controller for display command
        # throw_error(boolean) : controller for throw Exception when return code is not 0
        # output : stdout(string)
        # print stderr when stderr is not empty
        if show_command:
            print(command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = process.communicate()
        if process.returncode != 0:
            print(result[1])
            if throw_error:
                raise Exception('[Error] Run command returns stderr')
        return result[0]

    @classmethod
    def get_job_list_from_build(cls, data_site, build_path):
        # data_site(string) : for adjusting reference data path and database suffix
        # build_path(string) : build path for specific reference data path
        # output : [["hourly",[jobs]],["daily",[jobs]],["weekly",[jobs]]], {'oozie job name' : 'flag path'}
        # for DP2, output just left hourly jobs. just keep this structure for flexible.
        output_element = "dp2"
        if data_site == "beta":
            output_element = output_element + "-beta"
        output_path = build_path + "/output/" + output_element
        flags = dict()
        hourly_jobs = list()
        daily_jobs = list()
        table_jobs = cls.run_command("ls -d %s/oozie/t*" % output_path).split()
        for job_path in table_jobs:
            job = job_path.split('/')[-1]
            frequency = cls.run_command("grep 'coordExecFreq=' %s/job.properties | tail -n 1" % job_path)
            flag = cls.run_command("grep 'TARGET_FLAG=' %s/job.properties | cut -d'=' -f2" % job_path)[:-1]
            if not flag:
                raise Exception('[Error] Flag is empty')
            if data_site == "production":
                if "cam" in job:
                    flag_path_prefix = cls.AWS_PROD_CAM_PATH
                elif "ncie" in job or "dp2_major_object_counts" in job:
                    flag_path_prefix = cls.AWS_PROD_MISC_PATH
                else:
                    flag_path_prefix = cls.AWS_PROD_SHN_PATH
            else:
                if "cam" in job:
                    flag_path_prefix = cls.AWS_BETA_CAM_PATH
                else:
                    flag_path_prefix = cls.AWS_BETA_SHN_PATH
            flags[job] = '%s/%s' % (flag_path_prefix, flag)
            if "hours(1)" in frequency:
                hourly_jobs.append(job)
            elif "days(1)" in frequency:
                if "router_device_activity_daily" in job:
                    continue
                daily_jobs.append(job)
            else:
                raise Exception('[Error] frequency out of excepted: %s' % frequency)
        return [["hourly", hourly_jobs], ["daily", daily_jobs]], flags

    @classmethod
    def get_build(cls, mode="verified", version=""):
        # mode(string) : for switch build source folder
        # version(string) : for get build with specific version
        # output : build_folder(string), build_version(string)
        if mode == "test":
            s3_build_path = cls.AWS_TESTING_BUILD_PATH
        else:
            s3_build_path = cls.AWS_VERIFIED_BUILD_PATH

        if not version:
            build_file = cls.run_command("aws s3 ls %s/ | grep 'SHN-Data-Pipeline' | sort | tail -1 | awk '{print $4}'"
                                         % s3_build_path)[:-1]
        else:
            build_file = cls.run_command(
                "aws s3 ls %s/ | grep 'SHN-Data-Pipeline' | grep '%s' | sort | tail -1 | awk '{print $4}'"
                % (s3_build_path, version))[:-1]
        if not build_file:
            raise Exception('[Error] No available build to deploy')
        cls.run_command("aws s3 cp %s/%s /home/hadoop/" % (s3_build_path, build_file))
        cls.run_command("tar -C /home/hadoop/ -zxvf /home/hadoop/%s" % build_file)
        return "/home/hadoop/%s" % build_file.split('.tar')[0], build_file.split('.tar')[0].split('1.0.')[1]

    @classmethod
    def config_env(cls, data_site, folder, version, prefix="function", concurrency=1, timeout=28800):
        # data_site(string) : for judge to configure as production, beta or test site
        # folder(string) : target build folder that will be configured
        # version(string) : for adding oozie job name suffix name to identify easier
        if data_site == "production":
            prod_env_path = "%s/output/dp2/op-utils/set-env.sh" % folder
            cls.run_command("sed -i '/OOZIE_APP_EXT/d' %s" % prod_env_path)
            cls.run_command("sed -i '7a OOZIE_APP_EXT=.AWS_Production%s_DP2' %s" % (version, prod_env_path))
            cls.run_command("sed -i 's/export BACKUP_DOMAIN/#export BACKUP_DOMAIN/g' %s" % prod_env_path)
        elif data_site == "beta":
            beta_env_path = "%s/output/dp2-beta/op-utils/set-env.sh" % folder
            cls.run_command("sed -i '/OOZIE_APP_EXT/d' %s " % beta_env_path)
            cls.run_command("sed -i '7a OOZIE_APP_EXT=.AWS_Beta%s_DP2' %s" % (version, beta_env_path))
            cls.run_command("sed -i 's/export BACKUP_DOMAIN/#export BACKUP_DOMAIN/g' %s" % beta_env_path)
        elif data_site == "test":
            prod_env_path = "%s/output/dp2/op-utils/set-env.sh" % folder
            beta_env_path = "%s/output/dp2-beta/op-utils/set-env.sh" % folder
            prod_oozie_folder = "%s/output/dp2/oozie" % folder
            beta_oozie_folder = "%s/output/dp2/oozie" % folder
            cls.run_command("sed -i 's/180/%s/g' %s" % (timeout, prod_env_path))
            cls.run_command("sed -i 's/180/%s/g' %s" % (timeout, beta_env_path))
            cls.run_command("sed -i 's/concurrency=./concurrency=%i/g' %s/*/job.properties" %
                            (concurrency, prod_oozie_folder))
            cls.run_command("sed -i 's/concurrency=./concurrency=%i/g' %s/*/job.properties" %
                            (concurrency, beta_oozie_folder))
            cls.run_command("sed -i '/export DB_PREFIX/d' %s" % prod_env_path)
            cls.run_command("sed -i '/export DB_PREFIX/d' %s" % beta_env_path)
            cls.run_command("sed -i '6a export DB_PREFIX=%s_' %s" % (prefix, prod_env_path))
            cls.run_command("sed -i '6a export DB_PREFIX=%s_' %s" % (prefix, beta_env_path))
            cls.run_command("sed -i '/OOZIE_APP_EXT/d' %s " % prod_env_path)
            cls.run_command("sed -i '/OOZIE_APP_EXT/d' %s " % beta_env_path)
            cls.run_command("sed -i '7a export OOZIE_APP_EXT=.AWS_Test%s' %s" % (version, prod_env_path))
            cls.run_command("sed -i '7a export OOZIE_APP_EXT=.AWS_Test%s' %s" % (version, beta_env_path))
            cls.run_command("sed -i 's/export BACKUP_DOMAIN/#export BACKUP_DOMAIN/g' %s" % prod_env_path)
            cls.run_command("sed -i 's/export BACKUP_DOMAIN/#export BACKUP_DOMAIN/g' %s" % beta_env_path)

    @classmethod
    def create_bucket(cls, prefix="function"):
        if "%s-dp-shn" % prefix not in cls.run_command("aws s3 ls"):
            cls.run_command("aws s3 mb s3://%s-dp-shn-us-west-2" % prefix)
            cls.run_command("aws s3 mb s3://%s-dp-cam-us-west-2" % prefix)
            cls.run_command("aws s3 mb s3://%s-dp-sig-us-west-2" % prefix)
            cls.run_command("aws s3 mb s3://%s-dp-misc-us-west-2" % prefix)

    @classmethod
    def set_job_time(cls, data_site, folder, jobs, flags):
        # data_site(string) : transport to methods
        # folder(string)  : transport to methods
        # jobs(list) : transport to methods, [0]=hourly, [1]=daily, [2]=weekly
        # flags(dict) : transport to methods
        # control flow for get oozie job start, end time list and export to config file
        job_time_list = list()
        job_time_list.append("#hourly jobs")
        job_time_list.extend(cls.get_next_start_time(data_site, folder, flags, jobs[0]))
        job_time_list.append("#daily jobs")
        job_time_list.extend(cls.get_next_start_time(data_site, folder, flags, jobs[1]))
        cls.export_app_time(data_site, job_time_list, folder)

    @staticmethod
    def export_app_time(data_site, job_time_list, build_path):
        # data_site(string) : for switch output folder as production/beta
        # job_time_list(list) : contains each job name, start time and end time
        # folder : build folder
        # export oozie job start and end time to app-time.conf
        if data_site == "production":
            path_element = "dp2"
        else:
            path_element = "dp2-beta"
        output_path = "%s/output/%s/op-utils" % (build_path, path_element)
        job_time_file = open("%s/app-time.conf" % output_path, "w")
        for line in job_time_list:
            job_time_file.write(line + "\n")
        job_time_file.close()

    @classmethod
    def get_next_start_time(cls, data_site, build_path, flags, jobs):
        # data_site(string): for switch search latest flag s3 path ,and output path
        # build_path(string): build path
        # flags(dict): oozie job name and flag path mapping table. ex: 'T1Device': '<path_without_bucket>'
        # jobs(list): oozie job frequency and oozie job name mapping table
        # output : oozie next job time(list)
        # [0][1]=hourly jobs, [1][1]=daily jobs, [2][1]=weekly jobs
        job_time_list = []
        if data_site == "production":
            reference_path = "dp2"
        else:
            reference_path = "dp2-beta"
        # oozie job start time executes previous hour/day/week partition
        # if we got flag h=09, next job is h=10, so oozie job start time needs to be configured as 11:00(+2h)

        for job in jobs[1]:
            add_time = timedelta(days=2) if 'daily' in job else timedelta(hours=2)

            # get oozie job start time minutes from original app-time.conf
            flag_minute = cls.run_command(
                "cat %s/output/%s/op-utils/app-time.conf | grep '%s' | grep coordStart | head -1 " % (
                    build_path, reference_path, job))[-4:-1]
            if not re.match('\d{2}Z', flag_minute):
                raise Exception('[Error] Get malformed minute from app-time.conf:', flag_minute)

            # get oozie job start time date from flag path
            # if oozie job is a new job cause no flag, setting now time as next start time
            flag_year = cls.run_command("aws s3 ls %s/ | tail -1 | awk '{print $4}'" % flags[job])[2:6]
            flag_month = cls.run_command(
                "aws s3 ls %s/y=%s/ | tail -1 | awk '{print $4}'" % (flags[job], flag_year))[2:4]
            flag_day = cls.run_command("aws s3 ls %s/y=%s/m=%s/ | tail -1 | awk '{print $4}'" %
                                       (flags[job], flag_year, flag_month))[2:12]
            if 'hourly' in job:
                flag_hour = cls.run_command("aws s3 ls %s/y=%s/m=%s/d=%s/ | tail -1 | awk '{print $4}'" %
                                            (flags[job], flag_year, flag_month, flag_day))[2:4]
                if not re.match('\d{4}-\d{2}-\d{2}', flag_day) or not re.match('\d{2}', flag_hour):
                    print('[Error] Get malformed time from s3:d=%s, h=%s' % (flag_day, flag_hour))
                    flag_day = datetime.now().strftime('%Y-%m-%d')
                    flag_hour = datetime.now().strftime('%H')
                print('Last f_flag date: %s, hour: %s' % (flag_day, flag_hour))
                job_start_time = datetime.strptime(flag_day + flag_hour, '%Y-%m-%d%H') + add_time
                job_end_time = job_start_time + timedelta(days=36524)
                job_time_list.append(
                    "%s:    coordStart=%s:%s" % (job, job_start_time.strftime('%Y-%m-%dT%H'), flag_minute))
                print(job_time_list[-1])
                job_time_list.append("%s:    coordEnd=%s:00Z" % (job, job_end_time.strftime('%Y-%m-%dT%H')))
                print(job_time_list[-1])
            else:
                if not re.match('\d{4}-\d{2}-\d{2}', flag_day):
                    print('[Error] Get malformed time from s3:d=%s' % flag_day)
                    flag_day = datetime.now().strftime('%Y-%m-%d')
                print('Last f_flag date: %s' % flag_day)
                job_start_time = datetime.strptime(flag_day, '%Y-%m-%d') + add_time
                job_end_time = job_start_time + timedelta(days=36524)
                job_time_list.append(
                    "%s:    coordStart=%s:%s" % (job, job_start_time.strftime('%Y-%m-%dT00'), flag_minute))
                print(job_time_list[-1])
                job_time_list.append("%s:    coordEnd=%s:00Z" % (job, job_end_time.strftime('%Y-%m-%dT02')))
                print(job_time_list[-1])
        return job_time_list

    @classmethod
    def deploy_build(cls, data_site, build_path, suspend_jobs=list(), change_build=False):
        # data_site(string) : for switch configured environment folder path
        # path(string) : build path
        # suspend_jobs(list) : suspended oozie jobs ID list
        # change_build(boolean) : for control deploy flow will enter job recover or not
        # if deploy failed, suspended jobs will be resumed, otherwise will be killed
        if data_site == "production":
            target_folder = "dp2"
        else:
            target_folder = "dp2-beta"
        deploy_folder = "%s/output/%s/op-utils" % (build_path, target_folder)
        try:
            cls.run_command("bash %s/deploy.sh all" % deploy_folder, throw_error=False)
            cls.run_command("bash %s/run-jobs.sh" % deploy_folder)
        except Exception:
            if change_build:
                print('[Error] Deploy failed, resume previous jobs')
                cls.resume_all_job(suspend_jobs)
            exit(1)
        else:
            if change_build:
                cls.kill_all_job(suspend_jobs)

    @classmethod
    def wait_and_suspend_all_jobs(cls, oozie_job_list):
        # oozie_job_list(dict) : {oozie_job_id:[next_start_time, oozie_job_name]}
        # output : oozie_job_list(dict), all jobs has need suspended
        suspended_jobs_count = 1
        # scan all job in job list until all jobs has been suspended
        while True:
            for oozie_job in oozie_job_list:
                # judge single oozie job status is running or suspended
                cannot_suspended_job = cls.run_command(
                    "oozie job -info %s | grep oozie-oozi-C@ | grep 'RUNNING\|SUSPENDED'" %
                    (oozie_job_list[oozie_job][0]), throw_error=False, show_command=False)
                # judge whole oozie jos status is suspended
                cannot_suspended_status = cls.run_command("oozie job -info %s | grep 'Status' | grep 'SUSPENDED'" %
                                                          (oozie_job_list[oozie_job][0]), throw_error=False,
                                                          show_command=False)
                # if no running single jobs and whole job not be suspended yet, suspend it
                # suspend a running oozie job won't suspend yarn process, yarn still processing until finish
                if not (cannot_suspended_job or cannot_suspended_status):
                    print('=== Suspending %s (%d/%d) ===' % (oozie_job, suspended_jobs_count, len(oozie_job_list)))
                    cls.run_command("oozie job -suspend %s" % oozie_job_list[oozie_job][0])
                    suspended_jobs_count += 1
                if suspended_jobs_count > len(oozie_job_list):
                    break
            if suspended_jobs_count > len(oozie_job_list):
                break
        return oozie_job_list

    @classmethod
    def kill_all_job(cls, oozie_job_list):
        # oozie_job_list(dict) : {oozie_job_id:[next_start_time, oozie_job_name]}
        print('=== Kill All Jobs (Count: %s) ===' % len(oozie_job_list))
        for oozie_job in oozie_job_list:
            cls.run_command("oozie job -kill %s" % oozie_job_list[oozie_job][0])

    @classmethod
    def suspend_all_job(cls, oozie_job_list):
        # oozie_job_list(dict) : {oozie_job_id:[next_start_time, oozie_job_name]}
        print('=== Suspend All Jobs ===')
        for oozie_job in oozie_job_list:
            cls.run_command("oozie job -suspend %s" % oozie_job_list[oozie_job][0])

    @classmethod
    def resume_all_job(cls, oozie_job_list):
        # oozie_job_list(dict) : {oozie_job_id:[next_start_time, oozie_job_name]}
        print('=== Resume All Jobs ===')
        for oozie_job in oozie_job_list:
            cls.run_command("oozie job -resume %s" % oozie_job_list[oozie_job][0])

    @classmethod
    def get_job_list(cls, job_name, show_suspend=True):
        # job_name(string) : oozie job name, ex: T1Device
        # output : oozie_job_list(dict) : {oozie_job_id:[next_start_time, oozie_job_name]}
        # get running/prepare/suspend jobs list
        print('\nCurrent status of Oozie job:')
        if job_name == "all":
            job_name = ""
        if show_suspend:
            oozie_job_info = cls.run_command(
                "oozie jobs info -jobtype coordinator -len 5000|grep '%s.*RUNNING\|%s.*PREP\|%s.*SUSPEND'|sort -k8" %
                (job_name, job_name, job_name), show_command=False)[:-1].rstrip('\n').split('\n')
        else:
            oozie_job_info = cls.run_command(
                "oozie jobs info -jobtype coordinator -len 5000|grep '%s.*RUNNING\|%s.*PREP'|sort -k8" %
                (job_name, job_name), show_command=False)[:-1].rstrip('\n').split('\n')
        print("JobID\t\t\t\t     Next Materialized    App Name")
        oozie_job_list = {}
        for each_job in oozie_job_info:
            result = re.findall('(.*-oozie-oozi-C)[ ]*(%s.*)\.[\S ]*.*GMT    ([0-9: -]*).*    ' % job_name, each_job)
            if len(result) > 0:
                print(result[0][0], result[0][2], result[0][1])
                oozie_job_list.update({result[0][1]: [result[0][0], result[0][2]]})
        print('Total jobs: %s' % len(oozie_job_list))
        print('\nCurrent time: %s' % datetime.now())
        return oozie_job_list

    @classmethod
    def check_job_status(cls, job_name, oozie_job_list):
        # job_name(string) : oozie job name, ex: T1Device
        # oozie_job_list(dict) : {oozie_job_id:[next_start_time, oozie_job_name]}
        # get each job status and focus on waiting, suspend, killed jobs
        jobs_to_hide = '\|SUCCEEDED\|READY'
        all_job_status = ''
        if job_name == "all":
            jobs_count = 1
            for job_name in oozie_job_list:
                print('\n=== Job Checking(%d/%d) ===' % (jobs_count, len(oozie_job_list)))
                job_status = cls.run_command(
                    "oozie job -info %s -len 5000|grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" %
                    (oozie_job_list[job_name][0], jobs_to_hide), show_command=False)
                print(job_status)
                all_job_status += job_status
                jobs_count += 1
        else:
            if job_name in oozie_job_list:
                print('=== Job Checking ===')
                all_job_status = cls.run_command("oozie job -info %s |grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" %
                                                 (oozie_job_list[job_name][0], jobs_to_hide), show_command=False)
                print(all_job_status)
            else:
                print('Job not found in Oozie job list')
        return all_job_status

    @classmethod
    def add_cronjob(cls, data_site, build_path):
        # data_site(string) : for cronjob site parameter
        # build_path(string) : build path for getting update signature tool
        # only using for new production/beta site deploy
        # these 2 site needs update signature and send notification when geoip update finish
        cronjob_file = "/home/hadoop/cron_temp"
        cls.run_command("crontab -l > %s" % cronjob_file)
        signature_cronjob = cls.run_command("cat %s | grep 'update_signature/bg_executor.sh'" % cronjob_file,
                                            throw_error=False)
        geoip_cronjob = cls.run_command("cat %s | grep 'update_geoip/geoip_bg_executor_with_mail.sh'" % cronjob_file,
                                        throw_error=False)
        tmufe_cronjob = cls.run_command("cat %s | grep 'update_tmufe/bg_executor.sh'" % cronjob_file,
                                        throw_error=False)
        timedout_cronjob = cls.run_command("cat %s | grep 'timedout_monitor/timedout_monitor.sh'" % cronjob_file,
                                           throw_error=False)
        stunnel_cronjob = cls.run_command("cat %s | grep 'backup_stunnel/backup_stunnel.sh'" % cronjob_file,
                                          throw_error=False)
        clean_cronjob = cls.run_command("cat %s | grep 'clean_joblog/clean_joblog.sh'" % cronjob_file,
                                        throw_error=False)
        geoip_sync_cronjob = cls.run_command("cat %s | grep 'spn-data-us-west-2/dataset/GeoIP/'" % cronjob_file,
                                             throw_error=False)
        # before run this method, cronjob has not update signature cronjob
        cls.run_command("mkdir -p /home/hadoop/op/")
        if data_site == "production" and not signature_cronjob:
            cls.run_command("cp -r %s/QA/dp2/update_signature %s/" % (build_path, cls.OP_PATH))
            cls.run_command("echo '*/30 * * * * %s/update_signature/bg_executor.sh %s' >> %s " %
                            (cls.OP_PATH, data_site, cronjob_file))
        # before run this method, cronjob has not update geoip cronjob
        if not geoip_cronjob:
            cls.run_command("cp -r %s/QA/dp2/update_geoip %s/" % (build_path, cls.OP_PATH))
            cls.run_command("echo '0 * * * * %s/update_geoip/geoip_bg_executor_with_mail.sh %s' >> %s " %
                            (cls.OP_PATH, data_site, cronjob_file))
        # before run this method, cronjob has not update tmufe cronjob
        if data_site == "production" and not tmufe_cronjob:
            cls.run_command("cp -r %s/QA/dp2/update_tmufe %s/" % (build_path, cls.OP_PATH))
            cls.run_command("echo '0 * * * * %s/update_tmufe/bg_executor.sh %s' >> %s " %
                            (cls.OP_PATH, data_site, cronjob_file))
        # before run this method, cronjob has not timedout monitor cronjob
        if not timedout_cronjob:
            cls.run_command("cp -r %s/QA/dp2/timedout_monitor %s/" % (build_path, cls.OP_PATH))
            cls.run_command("echo '*/30 * * * * %s/timedout_monitor/timedout_monitor.sh %s' >> %s " %
                            (cls.OP_PATH, data_site, cronjob_file))
        # before run this method, cronjob has not backup stunnel cronjob
        if not stunnel_cronjob:
            cls.run_command("cp -r %s/QA/dp2/backup_stunnel %s/" % (build_path, cls.OP_PATH))
            cls.run_command("echo '* */24 * * * %s/backup_stunnel/backup_stunnel.sh' >> %s " %
                            (cls.OP_PATH, cronjob_file))
        # before run this method, cronjob has not clean joblog cronjob
        if not clean_cronjob:
            cls.run_command("cp -r %s/QA/dp2/clean_joblog %s/" % (build_path, cls.OP_PATH))
            cls.run_command("echo '* */24 * * * %s/clean_joblog/clean_joblog.sh' >> %s " %
                            (cls.OP_PATH, cronjob_file))

        if data_site == "beta" and not geoip_sync_cronjob:
            cls.run_command(
                "echo '0 * * * * aws s3 sync s3://spn-data-us-west-2/dataset/GeoIP/ s3://dp-aws-services-files-production-us-west-2/geoip/ >> /home/hadoop/cron.log' >> %s " %
                cronjob_file)

        cls.run_command("crontab %s" % cronjob_file)
        cls.run_command("rm %s" % cronjob_file)

    @classmethod
    def disable_stunnel(cls):
        # search and kill stunned pid
        # using test data site to avoid send notification
        stunnel_pid = cls.run_command("ps -ef | grep [s]tunnel | awk '{print $2}'")
        if stunnel_pid:
            cls.run_command("ps -ef | grep [s]tunnel | awk '{print $2}' | xargs sudo kill -9", throw_error=False)

    @classmethod
    def check_database_table(cls, database, table):
        # database(string) : database name
        # table(string) : table name
        # check database and table name exists in FLAGS or not
        if database != "all" and database not in cls.FLAGS.keys():
            raise Exception('[Error] Invalid database name')
        elif table and table not in cls.FLAGS[database].keys():
            raise Exception('[Error] Invalid table name')

    @classmethod
    def repair_partition(cls, database="all", table=""):
        # database(string) : database name
        # table(string) : table name
        # clean *_$folder$ on table parquet file path and repair partitions

        cls.check_database_table(database, table)
        # repair all databases and all tables
        if database == "all":
            for database in cls.FLAGS.keys():
                for table in cls.FLAGS[database].keys():
                    if 'beta' in database:
                        cls.clean_fake_folder(database, table)
                    cls.run_command(
                        'beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' %
                        (database, table))
        # repair all tables in specific database
        elif not table:
            for table in cls.FLAGS[database].keys():
                if 'beta' in database:
                    cls.clean_fake_folder(database, table)
                cls.run_command(
                    'beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' %
                    (database, table))
        # repair specific table
        else:
            if 'beta' in database:
                cls.clean_fake_folder(database, table)
            cls.run_command(
                'beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' %
                (database, table))

    @classmethod
    def clean_fake_folder(cls, database, table):
        # database(string) : database name
        # table(string) : table name
        # clean *_$folder$ on table parquet file path
        # in dp2 and dp2_beta, table name : t_<table_name>, flag name : f_<table_name>
        if "misc" in database:
            aws_path = cls.AWS_PROD_MISC_PATH
        elif "dp_beta" in database and "cam" in database:
            aws_path = cls.AWS_BETA_CAM_PATH
        elif "dp_beta" in database:
            aws_path = cls.AWS_BETA_SHN_PATH
        elif "cam" in database:
            aws_path = cls.AWS_PROD_CAM_PATH
        else:
            aws_path = cls.AWS_PROD_SHN_PATH
        s3_folder = cls.FLAGS[database][table].replace('f_', 't_')
        # skip datalake because parquet file does not exists on our bucket
        cls.run_command("aws s3 rm %s/%s --recursive --exclude '*' --include '*folder*'" % (aws_path, s3_folder))

    @classmethod
    def check_missing_partitions(cls, database, table):
        # database(string) : database name
        # table(string) : table name
        # output : missing_partitions(list) [datetime]
        check_time = cls.START_TIME
        stepping_time = timedelta(hours=1) if 'hourly' in table else timedelta(days=1)
        missing_partitions = list()
        partition_list = cls.run_command(
            'beeline -u "jdbc:hive2://localhost:10000/" -e "show partitions %s.%s;"' % (database, table))
        # consider hourly partition may generating when user query at same hour, so end time will be set at 2 hours before
        time_suffix = timedelta(hours=2) if 'hourly' in table else timedelta(days=2)
        while check_time < datetime.now() - time_suffix:
            if 'hourly' in table:
                if check_time.strftime('d=%Y-%m-%d/h=%H') not in partition_list \
                        and check_time.strftime('pdd=%Y-%m-%d/phh=%H') not in partition_list \
                        and check_time.strftime('dt=%Y-%m-%d-%H') not in partition_list:
                    missing_partitions.append(check_time.strftime('date=%Y-%m-%d, hour=%H'))
            else:
                if check_time.strftime('d=%Y-%m-%d') not in partition_list \
                        and check_time.strftime('pdd=%Y-%m-%d') not in partition_list \
                        and check_time.strftime('dt=%Y-%m-%d') not in partition_list:
                    missing_partitions.append(check_time.strftime('date=%Y-%m-%d'))
            check_time += stepping_time
        return missing_partitions

    @classmethod
    def check_missing_flags(cls, database, table):
        # database(string) : database name
        # table(string) : table name
        # output : missing_partitions(list) [datetime]
        check_time = cls.START_TIME
        stepping_time = timedelta(hours=1) if 'hourly' in table else timedelta(days=1)
        missing_partitions = list()
        if database == "dp_shn":
            s3_path = cls.AWS_PROD_SHN_PATH
        elif database == "dp_cam":
            s3_path = cls.AWS_PROD_CAM_PATH
        elif database == "dp_misc":
            s3_path = cls.AWS_PROD_MISC_PATH
        elif database == "dp_beta_shn":
            s3_path = cls.AWS_BETA_SHN_PATH
        else:
            s3_path = cls.AWS_BETA_CAM_PATH
        partition_list = cls.run_command('aws s3 ls %s/%s/ --recursive' % (s3_path, cls.FLAGS[database][table]))
        # consider hourly partition may generating when user query at same hour, so end time will be set at 2 hours before
        time_suffix = timedelta(hours=2) if 'hourly' in table else timedelta(days=2)
        while check_time < datetime.now() - time_suffix:
            if 'hourly' in table:
                if check_time.strftime('d=%Y-%m-%d/h=%H_') not in partition_list:
                    missing_partitions.append(check_time.strftime('date=%Y-%m-%d, hour=%H'))
            else:
                if check_time.strftime('d=%Y-%m-%d_') not in partition_list:
                    missing_partitions.append(check_time.strftime('date=%Y-%m-%d'))
            check_time += stepping_time
        return missing_partitions

    @classmethod
    def get_missing_partitions(cls, database="all", table="", source="flag"):
        # database(string) : database name
        # table(string) : table name
        # source(string) : for switch to check from flag or partition list
        # output : missing_partitions(list) [datetime]
        cls.check_database_table(database, table)
        all_missing_partitions = dict()
        if database == "all":
            for database in cls.FLAGS.keys():
                for table in cls.FLAGS[database].keys():
                    if source == "flag" and database not in ['trs_src', 'datalake']:
                        all_missing_partitions[database + '.' + table] = cls.check_missing_flags(database, table)
                    else:
                        all_missing_partitions[database + '.' + table] = cls.check_missing_partitions(database, table)
        elif not table:
            for table in cls.FLAGS[database].keys():
                if source == "flag" and database not in ['trs_src', 'datalake']:
                    all_missing_partitions[database + '.' + table] = cls.check_missing_flags(database, table)
                else:
                    all_missing_partitions[database + '.' + table] = cls.check_missing_partitions(database, table)
        else:
            if source == "flag" and database not in ['trs_src', 'datalake']:
                all_missing_partitions[database + '.' + table] = cls.check_missing_flags(database, table)
            else:
                all_missing_partitions[database + '.' + table] = cls.check_missing_partitions(database, table)
        # print head and tail when item greater than DISPLAY_COUNT
        for item in all_missing_partitions.keys():
            if all_missing_partitions[item]:
                print(item + ' has %s missing partitions:' % len(all_missing_partitions[item]))
                if len(all_missing_partitions[item]) < cls.DISPLAY_COUNT:
                    for each in all_missing_partitions[item]:
                        print('\t' + each)
                else:
                    print('\t' + all_missing_partitions[item][0])
                    print('\t' + all_missing_partitions[item][1])
                    print('\t.....')
                    print('\t' + all_missing_partitions[item][-2])
                    print('\t' + all_missing_partitions[item][-1])
        return all_missing_partitions

    @classmethod
    def rerun_failed_jobs(cls, all_job_status):
        # all_job_status(string) : just like "python deploy_tool.py -c all" result
        job_list = [x.split()[:2] for x in (all_job_status.split('\n')[6:-1])]
        for job in job_list:
            if job[1] in "KILLED, TIMEDOUT":
                job_id, action_id = job[0].split('@')
                cls.run_command('oozie job -rerun %s -action %s' % (job_id, action_id))

    @classmethod
    def restart_hive_server(cls, suspend_jobs):
        cls.run_command('sudo stop hive-server2')
        cls.run_command('sudo start hive-server2')
        print('Sleep for 60 seconds for hive server restart')
        time.sleep(60)
        cls.resume_all_job(suspend_jobs)

    @classmethod
    def command_parser(cls):
        parser = argparse.ArgumentParser()
        action_group = parser.add_argument_group('Actions')
        action_group.add_argument("-s", dest="data_site", help="Choose deploy target data site")
        action_group.add_argument("-c", dest="check_job", help="Check Oozie job status")
        action_group.add_argument("-p", action="store_true", dest="check_partition", help="Check missing partition")
        action_group.add_argument("-r", action="store_true", dest="repair_partition", help="Repair partitions")
        action_group.add_argument("-R", action="store_true", dest="rerun", help="Rerun all KILLED/TIMEDOUT jobs")
        action_group.add_argument("--restart", action="store_true", dest="restart", help="Restart hive server2")
        oozie_action_group = parser.add_argument_group('Oozie actions')
        oozie_action_group.add_argument("-o", dest="oozie_action", help="Oozie action")
        partition_group = parser.add_argument_group('Parameters for check missing partition')
        partition_group.add_argument("--database", dest="database", help="Database name")
        partition_group.add_argument("--table", dest="table", help="Table name")
        partition_group.add_argument("--src", dest="source", default="flag", help="Check partition by flag or list")
        site_group = parser.add_mutually_exclusive_group()
        site_group.add_argument("-N", action="store_true", dest="new_deploy", help="Execute a new deploy on EMR")
        site_group.add_argument("-C", action="store_true", dest="change_build", help="Execute change build on EMR")
        test_env_group = parser.add_argument_group('Parameters for test data site environment')
        test_env_group.add_argument("-b", dest="build_name", help="Specify build name")
        test_env_group.add_argument("-t", type=int, dest="timeout", default="180", help="Set oozie job timeout")
        test_env_group.add_argument("--prefix", dest="prefix", default="function",
                                    help='Set database/s3 bucket name prefix')
        test_env_group.add_argument("--con", type=int, dest="concurrency", default=1, help="Set oozie jobs concurrency")
        if len(sys.argv) == 1:
            parser.print_help()
            print('\nQuick Start:')
            print('# Verified build location: %s' % cls.AWS_VERIFIED_BUILD_PATH)
            print('# Testing build location: %s' % cls.AWS_TESTING_BUILD_PATH)
            print('\n# To rerun all TIMEDOUT/KILLED jobs')
            print('python %s -R' % os.path.basename(__file__))
            print('\n# To deploy on a new EMR as Production data site')
            print('python %s -s production -N' % os.path.basename(__file__))
            print('\n# To deploy on a new EMR as Beta data site')
            print('python %s -s beta -N' % os.path.basename(__file__))
            print('\n# To change build on Production data site')
            print('python %s -s production -C' % os.path.basename(__file__))
            print('\n# To change build on Beta  data site')
            print('python %s -s beta -C' % os.path.basename(__file__))
            print('\n# To prepare testing build on current site')
            print(
                '\n# build_version=1.0.280, database_name=eric_shn_dp, bucket=s3://eric-shn-dp, timeout=28800 minutes, job concurrency=3')
            print('python %s -s test -b 280 --prefix eric -t 28800 --con 3' % os.path.basename(__file__))
            print('\n# To prepare testing build on current site with default value')
            print(
                '\n# build_version=latest version in testing build folder, database_name=function_shn_dp, bucket=s3://function-shn-dp, timeout=28800 minutes, job concurrency=1')
            print('python %s -s test' % os.path.basename(__file__))
            print('\n# To check all Oozie job status')
            print('python %s -c all' % os.path.basename(__file__))
            print('\n# To check specific Oozie job status')
            print('python %s -c T1Security' % os.path.basename(__file__))
            print('\n# To check partitions for specific table by show partitions')
            print('python %s -p --database dp --table t_router_hourly --src list' % os.path.basename(__file__))
            print('\n# To check partitions for specific table by f_ flag')
            print('python %s -p --database dp --table t_router_hourly' % os.path.basename(__file__))
            print('\n# To check partitions for all table in specific database')
            print('python %s -p --database dp' % os.path.basename(__file__))
            print('\n# To check partitions for all database')
            print('python %s -p' % os.path.basename(__file__))
            print('\n# To repair partitions for specific table')
            print('python %s -r --database dp --table t_device_hourly' % os.path.basename(__file__))
            print('\n# To repair partitions for all table in specific database')
            print('python %s -r --database dp' % os.path.basename(__file__))
            print('\n# To repair partitions for all database')
            print('python %s -r' % os.path.basename(__file__))
            print('\n# Notification : repair dp_beta on production account before staging and beta')
            print('\n# To repair partitions and clean fake folder on Beta data site specific table')
            print('python %s -r --database dp_beta --table t_router_hourly' % os.path.basename(__file__))
            print('\n# To repair partitions and clean fake folder on Beta data site')
            print('python %s -r --database dp_beta' % os.path.basename(__file__))
            print('\n# To restart hive server')
            print('python %s --restart' % os.path.basename(__file__))
            print('\n# To wait running and suspend all oozie jobs')
            print('python %s -o wait-suspend' % os.path.basename(__file__))
            print('\n# To suspend all running oozie jobs')
            print('python %s -o suspend' % os.path.basename(__file__))
            print('\n# To resume all oozie jobs')
            print('python %s -o resume' % os.path.basename(__file__))
            print('\n# To kill all oozie jobs')
            print('python %s -o kill' % os.path.basename(__file__))
            exit(0)
        return parser.parse_args()


if __name__ == "__main__":
    DT = DeployTool()
    main_job = DT.command_parser()
    if main_job.data_site:
        if main_job.data_site not in ("production", "beta", "test"):
            print('Please assign data site as "production", "beta" or "test".')
            exit()
        if main_job.data_site == "test":
            if main_job.build_name:
                build_folder, build_version = DT.get_build(version=main_job.build_name, mode="test")
            else:
                build_folder, build_version = DT.get_build(mode="test")
            # DT.create_bucket(prefix=main_job.prefix)
            DT.config_env(main_job.data_site, build_folder, build_version, prefix=main_job.prefix,
                          concurrency=main_job.concurrency, timeout=main_job.timeout)
            print('Testing build %s is ready to go' % build_version)
            print('Need to create database metadata')
            print('Need to import signature')
            print('Need to set oozie jobs start and end time')
            DT.disable_stunnel()
        else:
            if main_job.new_deploy:
                build_folder, build_version = DT.get_build()
                DT.add_cronjob(main_job.data_site, build_folder)
                DT.config_env(main_job.data_site, build_folder, build_version)
                all_jobs, flag_list = DT.get_job_list_from_build(main_job.data_site, build_folder)
                DT.set_job_time(main_job.data_site, build_folder, all_jobs, flag_list)
                DT.deploy_build(main_job.data_site, build_folder)
            elif main_job.change_build:
                build_folder, build_version = DT.get_build()
                DT.config_env(main_job.data_site, build_folder, build_version)
                all_jobs, flag_list = DT.get_job_list_from_build(main_job.data_site, build_folder)
                previous_jobs = DT.wait_and_suspend_all_jobs(DT.get_job_list("all"))
                DT.set_job_time(main_job.data_site, build_folder, all_jobs, flag_list)
                DT.deploy_build(main_job.data_site, build_folder, suspend_jobs=previous_jobs, change_build=True)
            else:
                print('Please using one of -N and -C after "-s production" and "-s beta"')
    elif main_job.repair_partition:
        if main_job.database and main_job.table:
            DT.repair_partition(database=main_job.database, table=main_job.table)
        elif main_job.database:
            DT.repair_partition(database=main_job.database)
        else:
            DT.repair_partition()
    elif main_job.check_job:
        DT.check_job_status(main_job.check_job, DT.get_job_list(main_job.check_job))
    elif main_job.check_partition:
        if main_job.source not in ["flag", "list"]:
            print('Please using "--src flag" or "--src list" to select source for check partitions')
            exit()
        if main_job.database and main_job.table:
            DT.get_missing_partitions(database=main_job.database, table=main_job.table, source=main_job.source)
        elif main_job.database:
            DT.get_missing_partitions(database=main_job.database, source=main_job.source)
        else:
            DT.get_missing_partitions(source=main_job.source)
    elif main_job.oozie_action:
        if main_job.oozie_action == "wait-suspend":
            DT.wait_and_suspend_all_jobs(DT.get_job_list("all"))
        elif main_job.oozie_action == "suspend":
            DT.suspend_all_job(DT.get_job_list("all"))
        elif main_job.oozie_action == "resume":
            DT.resume_all_job(DT.get_job_list("all"))
        elif main_job.oozie_action == "kill":
            DT.kill_all_job(DT.get_job_list("all"))
        else:
            print('Please using wait-suspend, suspend, resume, kill after -o')
    elif main_job.rerun:
        DT.rerun_failed_jobs(DT.check_job_status("all", DT.get_job_list("all")))
    elif main_job.restart:
        previous_jobs = DT.wait_and_suspend_all_jobs(DT.get_job_list("all", show_suspend=False))
        DT.restart_hive_server(previous_jobs)
    else:
        print('Please using -s <data_site>, -c <job>, --restart, -p, -r or -R')
