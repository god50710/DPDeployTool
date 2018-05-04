import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta


class DeployTool(object):
    START_TIME = datetime(2018, 1, 1)
    AWS_PROD_S3_PATH = "s3://trs-production-us-west-2"
    AWS_BETA_S3_PATH = "s3://trs-production-beta-data-us-west-2"
    AWS_VERIFIED_BUILD_PATH = "s3://eric-staging-us-west-2/build"
    AWS_TESTING_BUILD_PATH = "s3://eric-staging-us-west-2/test_build"
    AWS_SIGNATURE_PATH = "s3://eric-staging-us-west-2/signature"
    TOOL_VERSION = "20180504"
    FLAG = {'datalake': {'akamai_rgom': 'Application/shnprj_spn/hive/datalake.db/f_akamai_rgom',
                         'akamai_web': 'Application/shnprj_spn/hive/datalake.db/f_akamai_web'},
            'dp': {'e_ddi_001_parquet': 'Application/shnprj_spn/hive/dp.db/f_ddi_hourly',
                   'e_ncie_001_parquet': 'Application/shnprj_spn/hive/dp.db/f_ncie_hourly',
                   'e_routerinfo_001_parquet': 'Application/shnprj_spn/hive/dp.db/f_routerinfo_hourly',
                   'e_routerstat_001_parquet': 'Application/shnprj_spn/hive/dp.db/f_routerstat_hourly',
                   'e_tmis_cam_001_parquet': 'Application/shnprj_spn/hive/dp.db/f_tmis_cam_hourly',
                   't_cam_bfld_hourly': 'Application/shnprj_spn/hive/dp.db/f_cam_bfld_hourly',
                   't_cam_info_hourly': 'Application/shnprj_spn/hive/dp.db/f_cam_info_hourly',
                   't_cam_security_hourly': 'Application/shnprj_spn/hive/dp.db/f_cam_security_hourly',
                   't_cam_session_hourly': 'Application/shnprj_spn/hive/dp.db/f_cam_session_hourly',
                   't_cam_stat_hourly': 'Application/shnprj_spn/hive/dp.db/f_cam_stat_hourly',
                   't_cam_trs_hourly': 'Application/shnprj_spn/hive/dp.db/f_cam_trs_hourly',
                   't_device_hourly': 'Application/shnprj_spn/hive/dp.db/f_device_hourly',
                   #  't_device_session_hourly': 'Application/shnprj_spn/hive/dp.db/f_device_session_hourly',
                   't_router_hourly': 'Application/shnprj_spn/hive/dp.db/f_router_hourly',
                   't_router_security_hourly': 'Application/shnprj_spn/hive/dp.db/f_router_security_hourly',
                   't_security_hourly': 'Application/shnprj_spn/hive/dp.db/f_security_hourly',
                   't_cam_rule_daily': 'Application/shnprj_spn/hive/dp.db/f_cam_rule_daily',
                   't_rule_daily': 'Application/shnprj_spn/hive/dp.db/f_rule_daily',
                   't_traffic_daily': 'Application/shnprj_spn/hive/dp.db/f_traffic_daily',
                   't_cam_collection_daily': 'Application/shnprj_spn/hive/dp.db/f_cam_collection_daily',
                   't_cam_ips_hit_rule_collection_daily':
                       'Application/shnprj_spn/hive/dp.db/f_cam_ips_hit_rule_collection_daily',
                   't_device_collection_daily': 'Application/shnprj_spn/hive/dp.db/f_device_collection_daily',
                   't_router_device_daily': 'Application/shnprj_spn/hive/dp.db/f_router_device_daily',
                   't_ips_hit_rule_collection_daily':
                       'Application/shnprj_spn/hive/dp.db/f_ips_hit_rule_collection_daily',
                   't_traffic_stats_daily': 'Application/shnprj_spn/hive/dp.db/f_traffic_stats_daily',
                   't_cam_collection_weekly': 'Application/shnprj_spn/hive/dp.db/f_cam_collection_weekly',
                   't_router_collection_weekly': 'Application/shnprj_spn/hive/dp.db/f_router_collection_weekly',
                   't_rule_stats_weekly': 'Application/shnprj_spn/hive/dp.db/f_rule_stats_weekly'},
            'trs_src': {'akamai_malicious_20171218': 'trs_src/f_akamai_malicious_20171218',
                        'ddi_001_20171218': 'trs_src/f_ddi_001_20171218',
                        'ips_20171218': 'trs_src/f_ips_20171218',
                        'ncie_001_20171218': 'trs_src/f_ncie_001_20171218',
                        'router_security_20171218': 'trs_src/f_router_security_20171218'},
            'pm_src': {'t_dpi_config_stats_by_brand_weekly':
                           'Application/shnprj_spn/hive/pm_src.db/f_dpi_config_stats_by_brand_weekly',
                       't_dpi_config_stats_by_country_weekly':
                           'Application/shnprj_spn/hive/pm_src.db/f_dpi_config_stats_by_country_weekly',
                       't_dpi_config_stats_raw_weekly':
                           'Application/shnprj_spn/hive/pm_src.db/f_dpi_config_stats_raw_weekly',
                       't_ips_hourly': 'Application/shnprj_spn/hive/pm_src.db/f_ips_hourly',
                       't_ips_stat_daily_180d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=180d',
                       't_ips_stat_daily_1d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=1d',
                       't_ips_stat_daily_30d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=30d',
                       't_ips_stat_daily_70d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=7d',
                       't_ips_stat_daily_90d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=90d',
                       },
            'dp_beta': {'e_routerinfo_001_parquet': 'Application/shnprj_spn/hive/dp_beta.db/f_routerinfo_hourly',
                        'e_routerstat_001_parquet': 'Application/shnprj_spn/hive/dp_beta.db/f_routerstat_hourly',
                        't_tmis_cam_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_tmis_cam_hourly',
                        't_cam_bfld_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_bfld_hourly',
                        't_cam_info_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_info_hourly',
                        't_cam_security_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_security_hourly',
                        't_cam_session_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_session_hourly',
                        't_cam_stat_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_stat_hourly',
                        't_cam_trs_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_trs_hourly',
                        't_device_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_device_hourly',
                        't_device_session_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_device_session_hourly',
                        't_router_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_router_hourly',
                        't_router_security_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_router_security_hourly',
                        't_security_hourly': 'Application/shnprj_spn/hive/dp_beta.db/f_security_hourly',
                        't_cam_rule_daily': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_rule_daily',
                        't_rule_daily': 'Application/shnprj_spn/hive/dp_beta.db/f_rule_daily',
                        't_traffic_daily': 'Application/shnprj_spn/hive/dp_beta.db/f_traffic_daily',
                        't_cam_collection_daily': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_collection_daily',
                        't_cam_ips_hit_rule_collection_daily':
                            'Application/shnprj_spn/hive/dp_beta.db/f_cam_ips_hit_rule_collection_daily',
                        't_device_collection_daily': 'Application/shnprj_spn/hive/dp_beta.db/f_device_collection_daily',
                        't_router_device_daily': 'Application/shnprj_spn/hive/dp_beta.db/f_router_device_daily',
                        't_ips_hit_rule_collection_daily':
                            'Application/shnprj_spn/hive/dp_beta.db/f_ips_hit_rule_collection_daily',
                        't_traffic_stats_daily': 'Application/shnprj_spn/hive/dp_beta.db/f_traffic_stats_daily',
                        't_cam_collection_weekly': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_collection_weekly',
                        't_router_collection_weekly':
                            'Application/shnprj_spn/hive/dp_beta.db/f_router_collection_weekly',
                        't_rule_stats_weekly': 'Application/shnprj_spn/hive/dp_beta.db/f_rule_stats_weekly'}
            }

    @staticmethod
    def run_command(command, show_command=True, throw_error=True):
        # cmd : command we want to execute on shell
        # show_command(boolean) : display command
        # throw_error(boolean) : throw Exception when stderr is not empty
        # output : stdout(string)
        # print stderr when stderr is not empty
        if show_command:
            print(command)
        o = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = o.communicate()
        if result[1] != "":
            print(result)
            if throw_error:
                raise Exception('[Error] Run command returns stderr')
        else:
            return result[0]

    @classmethod
    def get_job_list_from_build(cls, data_site, folder):
        # data_site : for adjusting reference data path and database suffix
        # folder : build folder for specific reference data path
        # output : [["hourly",[jobs]],["daily",[jobs]],["weekly",[jobs]]], {'oozie job name' : 'flag path'}
        output_path = "output/data-pipeline-aws"
        if data_site == "beta":
            output_path = output_path + "-beta"
        output_path = folder + "/" + output_path
        flags = dict()
        hourly = list()
        daily = list()
        weekly = list()
        flag = ""
        # system monitor jobs has no f_flag, add to mapping list with empty directly
        system_jobs = cls.run_command("ls -d %s/oozie/System*" % output_path).split()
        for job in system_jobs:
            job = job.split('/')[-1]
            flags[job] = ''
            daily.append(job)
        table_jobs = cls.run_command("ls -d %s/oozie/T*" % output_path).split()
        for job_path in table_jobs:
            job = job_path.split('/')[-1]
            frequency = cls.run_command("grep 'coordExecFreq=' %s/job.properties | tail -n 1" % job_path)
            data_out = cls.run_command("grep 'dataOut=' %s/job.properties | tail -n 1 | cut -d'=' -f2" %
                                       job_path).split(':')
            for sub_string in data_out:
                if re.match('^f_\w*', sub_string):
                    flag = sub_string
                    break
            if not flag:
                raise Exception('[Error] Flag is empty')
            database = "dp"
            if "T0Datalake" in job:
                dataset_path = "%s/datasets/datalake.xml" % output_path
                database = "datalake"
            elif "T0" in job:
                dataset_path = "%s/datasets/dp-t0.xml" % output_path
            elif "T1" in job:
                dataset_path = "%s/datasets/dp-t1.xml" % output_path
            elif "T2" in job:
                dataset_path = "%s/datasets/dp-t2.xml" % output_path
            elif "TxExport" in job:
                dataset_path = "%s/datasets/trs_src.xml" % output_path
                database = "trs_src"
            elif "TxPmSrc" in job:
                dataset_path = "%s/datasets/pm_src.xml" % output_path
                database = "pm_src"
            else:
                raise Exception('[Error] dataset selection failed')
            if "1d" in flag:
                flag = "f_ips_stat_daily/period=1d"
            elif "7d" in flag:
                flag = "f_ips_stat_daily/period=7d"
            elif "30d" in flag:
                flag = "f_ips_stat_daily/period=30d"
            elif "90d" in flag:
                flag = "f_ips_stat_daily/period=90d"
            elif "180d" in flag:
                flag = "f_ips_stat_daily/period=180d"
            flag_path_prefix = cls.run_command("grep '%s' %s | grep 'uri-template'" %
                                               (flag, dataset_path)).split('${nameNode}/')[1].split('${')[0]
            if database != "trs_src":
                if data_site == "beta":
                    database = database + "_beta.db"
                else:
                    database = database + ".db"
            flags[job] = '%s%s/%s' % (flag_path_prefix, database, flag)
            if "hours(1)" in frequency:
                hourly.append(job)
            elif "days(1)" in frequency:
                daily.append(job)
            elif "days(7)" in frequency:
                weekly.append(job)
            else:
                raise Exception('[Error] frequency out of excepted: %s' % frequency)
        return [["hourly", hourly], ["daily", daily], ["weekly", weekly]], flags

    @classmethod
    def get_build(cls, mode="verified", build_name=""):
        # mode(string) : for switch build source folder
        # build_name(string) : for get build with specific version
        # output : build_folder(string), build_version(string)
        if mode == "test":
            s3_build_folder = cls.AWS_TESTING_BUILD_PATH
        else:
            s3_build_folder = cls.AWS_VERIFIED_BUILD_PATH

        if not build_name:
            build_file = cls.run_command("aws s3 ls %s/ | grep 'SHN-Data-Pipeline' | sort | tail -1 | awk '{print $4}'"
                                         % s3_build_folder)[:-1]
        else:
            build_file = cls.run_command(
                "aws s3 ls %s/ | grep 'SHN-Data-Pipeline' | grep '%s' | sort | tail -1 | awk '{print $4}'"
                % (s3_build_folder, build_name))[:-1]
        if not build_file:
            raise Exception('[Error] No available build to deploy')
        cls.run_command("aws s3 cp %s/%s /home/hadoop/" % (s3_build_folder, build_file))
        cls.run_command("tar -C /home/hadoop/ -zxvf /home/hadoop/%s" % build_file)
        return "/home/hadoop/%s" % build_file.split('.tar')[0], build_file.split('.tar')[0].split('1.0.')[1]

    @classmethod
    def config_env(cls, site, folder, version, suffix="function", concurrency=1, timeout=180, memory=True):
        # site(string) : for judge to configure as production, beta or test site
        # folder(string) : target build folder that will be configured
        # version(string) : for adding oozie job name suffix name to identify easier
        if site == "production":
            prod_env_path = "%s/output/data-pipeline-aws/op-utils/env" % folder
            cls.run_command("cp %s/aws-production.sh %s/$(whoami)\@$(hostname).sh" %
                            (prod_env_path, prod_env_path))
            cls.run_command("echo 'OOZIE_APP_EXT=.AWS_Production%s' >> %s/$(whoami)\@$(hostname).sh" %
                            (version, prod_env_path))
        elif site == "beta":
            # beta site running on a low-end site, needs to remove memory limitation
            beta_env_path = "%s/output/data-pipeline-aws-beta/op-utils/env" % folder
            beta_oozie_jobs_path = "%s/output/data-pipeline-aws-beta/oozie/*/job.properties" % folder
            beta_script_path = "%s/output/data-pipeline-aws-beta/script/hql_external_partition.sh" % folder
            beta_hql_path = "%s/output/data-pipeline-aws-beta/hql/*.hql" % folder
            cls.run_command(
                "cp %s/aws-production-beta-data.sh %s/$(whoami)\@$(hostname).sh" % (beta_env_path, beta_env_path))
            cls.run_command("echo 'OOZIE_APP_EXT=.AWS_Beta%s' >> %s/$(whoami)\@$(hostname).sh" %
                            (version, beta_env_path))
            cls.run_command("sed -i 's/^cntLowerbound=.*$/cntLowerbound=0/g' %s" % beta_oozie_jobs_path)
            cls.run_command("sed -i 's/ --driver-memory 12G --executor-memory 12G//g' %s" % beta_script_path)
            cls.run_command("sed -i '/SET hive.tez.java.opts=-Xmx10240m;/d' %s" % beta_hql_path)
            cls.run_command("sed -i '/SET hive.tez.container.size=12288;/d' %s" % beta_hql_path)
        elif site == "test":
            test_env_path = "%s/output/data-pipeline-aws/op-utils/env" % folder
            test_oozie_path = "%s/output/data-pipeline-aws/oozie" % folder
            test_script_path = "%s/output/data-pipeline-aws/script/hql_external_partition.sh" % folder
            test_hql_path = "%s/output/data-pipeline-aws/hql/*.hql" % folder
            test_env_make_path = "%s/src" % folder
            # test site needs remake source for adjust timeout, oozie job concurrency and import specific database name
            cls.run_command("cd %s; make clean" % test_env_make_path)
            # default timeout is 180 minutes
            if timeout != 180:
                cls.run_command("cd %s; sed -i 's/180/%s/g' data-pipeline/oozie/common.properties" %
                                (test_env_make_path, timeout))
            cls.run_command("cd %s; make %s-db" % (test_env_make_path, suffix))
            # default has memory limitation on T0 spark shell and part of T1 hql files
            if not memory:
                cls.run_command("sed -i 's/ --driver-memory 12G --executor-memory 12G//g' %s" % test_script_path)
                cls.run_command("sed -i '/SET hive.tez.java.opts=-Xmx10240m;/d' %s" % test_hql_path)
                cls.run_command("sed -i '/SET hive.tez.container.size=12288;/d' %s" % test_hql_path)
            cls.run_command("sed -i 's/concurrency=./concurrency=%i/g' %s/*/job.properties" %
                            (concurrency, test_oozie_path))
            cls.run_command("cp %s/hadoop\@ip-172-31-13-117.sh %s/$(whoami)\@$(hostname).sh" %
                            (test_env_path, test_env_path))
            cls.run_command("sed -i '/HADOOP_NAME_NODE/d' %s/$(whoami)\@$(hostname).sh" % test_env_path)
            cls.run_command("echo 'export HADOOP_NAME_NODE=s3://dp-%s' >> %s/$(whoami)\@$(hostname).sh" %
                            (suffix, test_env_path))
            cls.run_command("echo 'OOZIE_APP_EXT=.AWS_Test%s' >> %s/$(whoami)\@$(hostname).sh" %
                            (version, test_env_path))

    @classmethod
    def set_job_time(cls, site, folder, jobs, flags):
        # site(string) : transport to methods
        # folder(string)  : transport to methods
        # jobs(list) : transport to methods, [0]=hourly, [1]=daily, [2]=weekly
        # flags(dict) : transport to methods
        # control flow for get oozie job start, end time list and export to config file

        job_time_list = list()
        job_time_list.append("#hourly jobs")
        job_time_list.extend(cls.get_next_start_time(site, folder, flags, jobs[0]))
        job_time_list.append("#daily jobs")
        job_time_list.extend(cls.get_next_start_time(site, folder, flags, jobs[1]))
        job_time_list.append("#weekly jobs")
        job_time_list.extend(cls.get_next_start_time(site, folder, flags, jobs[2]))
        cls.export_app_time(site, job_time_list, folder)

    @staticmethod
    def export_app_time(site, job_time_list, folder):
        # site(string) : for switch output folder as production/beta
        # job_time_list(list) : contains each job name, start time and end time
        # folder : build folder
        # export oozie job start and end time to app-time.conf
        if site == "production":
            output_path = "data-pipeline-aws"
        else:
            output_path = "data-pipeline-aws-beta"
        deploy_folder = "%s/output/%s/op-utils" % (folder, output_path)
        job_time_file = open("%s/app-time.conf" % deploy_folder, "w")
        for line in job_time_list:
            job_time_file.write(line + "\n")
        job_time_file.close()

    @classmethod
    def get_next_start_time(cls, site, folder, flags, jobs):
        # site(string): for switch search latest flag s3 path ,and output path
        # folder(string): build folder
        # flags(dict): oozie job name and flag path mapping table. ex: 'T1Device': '<path_without_bucket>'
        # jobs(list): oozie job frequency and oozie job name mapping table
        # output : oozie next job time(list)
        # [0][1]=hourly jobs, [1][1]=daily jobs, [2][1]=weekly jobs
        job_time_list = []
        if site == "production":
            site_s3_path = cls.AWS_PROD_S3_PATH
            output_path = "data-pipeline-aws"
        else:
            site_s3_path = cls.AWS_BETA_S3_PATH
            output_path = "data-pipeline-aws-beta"
        # oozie job start time executes previous hour/day/week partition
        # if we got flag h=09, next job is h=10, so oozie job start time needs to be configured as 11:00(+2h)
        if jobs[0] == "hourly":
            add_time = timedelta(hours=2)
        elif jobs[0] == "daily":
            add_time = timedelta(days=2)
        else:
            add_time = timedelta(days=8)
        for job in jobs[1]:
            # get oozie job start time minutes from original app-time.conf
            f_flag_minute = cls.run_command(
                "cat %s/output/%s/op-utils/app-time.conf | grep '%s' | grep coordStart | head -1 " % (
                    folder, output_path, job))[-4:-1]
            if not re.match('\d{2}Z', f_flag_minute):
                raise Exception('[Error] Get malformed minute from app-time.conf:', f_flag_minute)

            # get oozie job start time date from flag path
            if "System" in job:
                f_flag_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                f_flag_day = cls.run_command("aws s3 ls %s/%s/ | tail -1 | awk '{print $4}' | cut -d'_' -f1" %
                                             (site_s3_path, flags[job]))[-11:-1]
            if not re.match('\d{4}-\d{2}-\d{2}', f_flag_day):
                raise Exception('[Error] Get malformed day from s3:', f_flag_day)

            # get oozie job start time hours from flag
            if jobs[0] == "hourly":
                if "TxExport" in job:
                    f_flag_hour = cls.run_command("aws s3 ls %s/%s/pdd=%s/ | tail -1 | awk '{print $4}'" %
                                                  (site_s3_path, flags[job], f_flag_day))[4:6]
                else:
                    f_flag_hour = cls.run_command("aws s3 ls %s/%s/d=%s/ | tail -1 | awk '{print $4}'" %
                                                  (site_s3_path, flags[job], f_flag_day))[2:4]
                if not re.match('\d{2}', f_flag_hour):
                    raise Exception('[Error] Get malformed hour from s3:', f_flag_hour)
            elif "System" in job:
                f_flag_hour = "02"
            else:
                f_flag_hour = "00"
            print('Last f_flag date: %s, hour: %s' % (f_flag_day, f_flag_hour))
            job_start_time = datetime.strptime(f_flag_day + f_flag_hour, '%Y-%m-%d%H') + add_time
            job_end_time = job_start_time + timedelta(days=36524)
            job_time_list.append(
                "%s:    coordStart=%s:%s" % (job, job_start_time.strftime('%Y-%m-%dT%H'), f_flag_minute))
            print(job_time_list[-1])
            job_time_list.append("%s:    coordEnd=%s:00Z" % (job, job_end_time.strftime('%Y-%m-%dT%H')))
            print(job_time_list[-1])
        return job_time_list

    @classmethod
    def deploy_build(cls, site, folder, suspend_jobs=list(), change_build=False):
        # site(string) : for switch configured environment folder path
        # folder(string) : build folder
        # suspend_jobs(list) : suspended oozie jobs ID list
        # change_build(boolean) : for control deploy flow will enter job recover or not
        # if deploy failed, suspended jobs will be resumed, otherwise will be killed
        if site == "production":
            target_folder = "data-pipeline-aws"
        else:
            target_folder = "data-pipeline-aws-beta"
        deploy_folder = "%s/output/%s/op-utils" % (folder, target_folder)
        try:
            cls.run_command("bash %s/deploy.sh all" % deploy_folder, throw_error=False)
            if site == "production":
                cls.run_command("sed -i '/DeviceSession/d' %s/run-jobs.sh" % deploy_folder)
            #  self.run_command("bash %s/run-jobs.sh" % deploy_folder)
            print("bash %s/run-jobs.sh" % deploy_folder)
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
        counter = 1
        while True:
            for oozie_job in oozie_job_list:
                cannot_suspend_job = cls.run_command(
                    "oozie job -info %s | grep oozie-oozi-C@ | grep 'RUNNING\|SUSPENDED'" %
                    (oozie_job_list[oozie_job][0]), show_command=False)
                cannot_suspend_status = cls.run_command("oozie job -info %s | grep 'Status' | grep 'SUSPENDED'" %
                                                        (oozie_job_list[oozie_job][0]), show_command=False)
                if not (cannot_suspend_job or cannot_suspend_status):
                    print('=== Suspending %s (%d/%d) ===' % (oozie_job, counter, len(oozie_job_list)))
                    cls.run_command("oozie job -suspend %s" % oozie_job_list[oozie_job][0])
                    counter += 1
                if counter > len(oozie_job_list):
                    break
            if counter > len(oozie_job_list):
                break
        return oozie_job_list

    @classmethod
    def kill_all_job(cls, oozie_job_list):
        print('=== Kill All Jobs (Count: %s) ===' % len(oozie_job_list))
        for oozie_job in oozie_job_list:
            cls.run_command("oozie job -kill %s" % oozie_job_list[oozie_job][0])

    @classmethod
    def suspend_all_job(cls, oozie_job_list):
        print('=== Suspend All Jobs ===')
        for oozie_job in oozie_job_list:
            cls.run_command("oozie job -suspend %s" % oozie_job_list[oozie_job][0])

    @classmethod
    def resume_all_job(cls, oozie_job_list):
        print('=== Resume All Jobs ===')
        for oozie_job in oozie_job_list:
            cls.run_command("oozie job -resume %s" % oozie_job_list[oozie_job][0])

    @classmethod
    def get_job_info(cls, target):
        print('\nCurrent status of Oozie job:')
        if target == "all":
            target = ""
        info = cls.run_command(
            "oozie jobs info -jobtype coordinator -len 5000|grep '%s.*RUNNING\|%s.*PREP\|%s.*SUSPEND'|sort -k8" %
            (target, target, target), show_command=False)[:-1].rstrip('\n').split('\n')
        print("JobID\t\t\t\t     Next Materialized    App Name")
        app_info = {}
        for each in info:
            result = re.findall('(.*-oozie-oozi-C)[ ]*(%s.*)\.[\S ]*.*GMT    ([0-9: -]*).*    ' % target, each)
            if len(result) > 0:
                print(result[0][0], result[0][2], result[0][1])
                app_info.update({result[0][1]: [result[0][0], result[0][2]]})
        print('Total jobs: %s' % len(app_info))
        print('\nCurrent time: %s' % datetime.now())
        return app_info

    @classmethod
    def check_job_status(cls, oozie_job, oozie_job_list):
        jobs_to_hide = '\|SUCCEEDED\|READY'
        if oozie_job == "all":
            counter = 1
            for oozie_job in oozie_job_list:
                print('\n=== Job Checking(%d/%d) ===' % (counter, len(oozie_job_list)))
                print(cls.run_command("oozie job -info %s -len 5000|grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" %
                                      (oozie_job_list[oozie_job][0], jobs_to_hide), show_command=False))
                counter += 1
        else:
            if oozie_job in oozie_job_list:
                print('=== Job Checking ===')
                print(cls.run_command("oozie job -info %s |grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" %
                                      (oozie_job_list[oozie_job][0], jobs_to_hide), show_command=False))
            else:
                print('Job not found in Oozie job list')

    @classmethod
    def add_cronjob(cls, site, folder):
        cronjob_file = "/home/hadoop/cron_temp"
        cls.run_command("crontab -l > %s" % cronjob_file)
        signature_cronjob = cls.run_command("cat %s | grep 'update_signature/bg_executor.sh'" % cronjob_file)
        geoip_cronjob = cls.run_command("cat %s | grep 'update_geoip/geoip_bg_executor.sh'" % cronjob_file)
        if not signature_cronjob:
            cls.run_command("cp -r %s/QA/update_signature /home/hadoop/" % folder)
            cls.run_command("echo '0 * * * * /home/hadoop/update_signature/bg_executor.sh %s' >> %s " %
                            (site, cronjob_file))
        if geoip_cronjob:
            cls.run_command("sed -i '/geoip_bg_executor.sh/d' %s" % cronjob_file)
            cls.run_command("echo '0 * * * * /trs/update_geoip/geoip_bg_executor_with_mail.sh %s' >> %s " %
                            (site, cronjob_file))
        cls.run_command("crontab %s" % cronjob_file)
        cls.run_command("rm %s" % cronjob_file)

    @classmethod
    def disable_stunnel(cls):
        current_job = cls.run_command("ps -ef | grep [s]tunnel | awk '{print $2}'")
        if current_job:
            cls.run_command("ps -ef | grep [s]tunnel | awk '{print $2}' | xargs sudo kill -9", throw_error=False)

    @classmethod
    def check_database_table(cls, database, table):
        if database != "all" and database not in cls.FLAG.keys():
            raise Exception('[Error] Invalid database name')
        elif table and table not in cls.FLAG[database].keys():
            raise Exception('[Error] Invalid table name')

    @classmethod
    def repair_partition(cls, database="all", table=""):
        cls.check_database_table(database, table)
        if database == "all":
            for database in cls.FLAG.keys():
                for table in cls.FLAG[database].keys():
                    # cls.run_command('beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' %(database, table))
                    cls.clean_fake_folder(database, table)
                    print('beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' % (
                        database, table))
        elif not table:
            for table in cls.FLAG[database].keys():
                # cls.run_command('beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' %(database, table))
                cls.clean_fake_folder(database, table)
                print('beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' % (
                    database, table))
        else:
            # cls.run_command('beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' %(database, table))
            cls.clean_fake_folder(database, table)
            print('beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "msck repair table %s.%s;"' %
                  (database, table))

    @classmethod
    def clean_fake_folder(cls, database, table):
        if database in ["dp", "dp_beta", "pm_src"]:
            s3_folder = cls.FLAG[database][table].replace('f_', 't_')
        elif database == "trs_src":
            s3_folder = cls.FLAG[database][table].replace('f_', '')
        else:
            return 0
        if database != "dp_beta":
            bucket = cls.AWS_PROD_S3_PATH
        else:
            bucket = cls.AWS_BETA_S3_PATH
        #   cls.run_command("aws s3 rm %s/%s --recursive --exclude '*' --include'*folder*'" % (bucket, s3_folder))
        print("aws s3 rm %s/%s --recursive --exclude '*' --include '*folder*'" % (bucket, s3_folder))

    @classmethod
    def check_missing_partitions(cls, database, table):
        check_time = cls.START_TIME
        if "daily" in table:
            time_unit = timedelta(days=1)
        elif "weekly" in table:
            time_unit = timedelta(days=7)
            while check_time.weekday() != 6:
                check_time += timedelta(days=1)
        else:
            time_unit = timedelta(hours=1)
        missing_partitions = list()
        partition_list = cls.run_command(
            'beeline -u "jdbc:hive2://localhost:10000/" --silent=true -e "show partitions %s.%s;"'
            % (database, table))
        while check_time < datetime.now() - timedelta(days=1):
            if time_unit == timedelta(hours=1):
                if check_time.strftime('d=%Y-%m-%d/h=%H') not in partition_list and \
                        check_time.strftime('pdd=%Y-%m-%d/phh=%H') not in partition_list and \
                        check_time.strftime('dt=%Y-%m-%d-%H') not in partition_list:
                    missing_partitions.append(check_time.strftime('date=%Y-%m-%d, hour=%H'))
            else:
                if check_time.strftime('d=%Y-%m-%d') not in partition_list and \
                        check_time.strftime('pdd=%Y-%m-%d') not in partition_list and \
                        check_time.strftime('dt=%Y-%m-%d') not in partition_list:
                    missing_partitions.append(check_time.strftime('date=%Y-%m-%d'))
            check_time += time_unit
        return missing_partitions

    # @classmethod
    # def find_current_build(cls):
    #     current_build = cls.run_command("find /home/hadoop/SHN-Data-Pipeline-* -maxdepth 0 -type d | sort | tail -1 ")
    #     if current_build:
    #         return current_build, current_build.split('1.0.')[1]
    #     else:
    #         return cls.get_build()

    @classmethod
    def check_missing_flags(cls, database, table):
        check_time = cls.START_TIME
        if "daily" in table:
            time_unit = timedelta(days=1)
        elif "weekly" in table:
            time_unit = timedelta(days=7)
            while check_time.weekday() != 6:
                check_time += timedelta(days=1)
        else:
            time_unit = timedelta(hours=1)
        missing_partitions = list()
        target_s3 = cls.AWS_PROD_S3_PATH
        if database == "dp_beta":
            target_s3 = cls.AWS_BETA_S3_PATH
        partition_list = cls.run_command('aws s3 ls %s/%s/ --recursive' %
                                         (target_s3, cls.FLAG[database][table]))
        while check_time < datetime.now() - timedelta(days=1):
            if time_unit == timedelta(hours=1):
                if check_time.strftime('d=%Y-%m-%d/h=%H_') not in partition_list and \
                        check_time.strftime('pdd=%Y-%m-%d/phh=%H_') not in partition_list and \
                        check_time.strftime('dt=%Y-%m-%d-%H_') not in partition_list:
                    missing_partitions.append(check_time.strftime('date=%Y-%m-%d, hour=%H'))
            else:
                if check_time.strftime('d=%Y-%m-%d_') not in partition_list and \
                        check_time.strftime('pdd=%Y-%m-%d_') not in partition_list and \
                        check_time.strftime('dt=%Y-%m-%d_') not in partition_list:
                    missing_partitions.append(check_time.strftime('date=%Y-%m-%d'))
            check_time += time_unit
        return missing_partitions

    @classmethod
    def get_missing_partitions(cls, database="all", table="", source="flag"):
        cls.check_database_table(database, table)
        all_missing_partitions = dict()
        if database == "all":
            for database in cls.FLAG.keys():
                for table in cls.FLAG[database].keys():
                    if source == "flag":
                        all_missing_partitions[database + '.' + table] = cls.check_missing_flags(database, table)
                    else:
                        all_missing_partitions[database + '.' + table] = cls.check_missing_partitions(database, table)
        elif not table:
            for table in cls.FLAG[database].keys():
                if source == "flag":
                    all_missing_partitions[database + '.' + table] = cls.check_missing_flags(database, table)
                else:
                    all_missing_partitions[database + '.' + table] = cls.check_missing_partitions(database, table)
        else:
            if source == "flag":
                all_missing_partitions[database + '.' + table] = cls.check_missing_flags(database, table)
            else:
                all_missing_partitions[database + '.' + table] = cls.check_missing_partitions(database, table)
        for item in all_missing_partitions.keys():
            if all_missing_partitions[item]:
                print(item + ' has %s missing partitions:' % len(all_missing_partitions[item]))
                if len(all_missing_partitions[item]) < 50:
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
    def fill_dependency(cls):
        pass

    @classmethod
    def command_parser(cls):
        parser = argparse.ArgumentParser()
        action_group = parser.add_argument_group('Actions')
        action_group.add_argument("-s", dest="site", help="Choose deploy target site")
        action_group.add_argument("-c", dest="check_job", help="Check Oozie job status")
        action_group.add_argument("-p", action="store_true", dest="check_partition", help="Check missing partition")
        action_group.add_argument("-r", action="store_true", dest="repair_partition", help="Repair partitions")
        partition_group = parser.add_argument_group('Parameters for check missing partition')
        partition_group.add_argument("--database", dest="database", help="Database name")
        partition_group.add_argument("--table", dest="table", help="Table name")
        partition_group.add_argument("--src", dest="source", default="flag", help="Check partition by flag or list")
        site_group = parser.add_mutually_exclusive_group()
        site_group.add_argument("-N", action="store_true", dest="new_deploy", help="Execute a new deploy on EMR")
        site_group.add_argument("-C", action="store_true", dest="change_build", help="Execute change build on EMR")
        test_env_group = parser.add_argument_group('Parameters for test site environment')
        test_env_group.add_argument("-b", dest="build_name", help="Specify build name")
        test_env_group.add_argument("-t", type=int, dest="timeout", default="180", help="Set oozie job timeout")
        test_env_group.add_argument("--suffix", dest="suffix", default="function",
                                    help='Set database/s3 bucket name suffix')
        test_env_group.add_argument("--con", type=int, dest="concurrency", default=1, help="Set oozie jobs concurrency")
        test_env_group.add_argument("-m", action="store_false", dest="memory", help="Remove hql memory limits")
        if len(sys.argv) == 1:
            parser.print_help()
            print('\nQuick Start:')
            print('# Verified build location: %s' % cls.AWS_VERIFIED_BUILD_PATH)
            print('# Testing build location: %s' % cls.AWS_TESTING_BUILD_PATH)
            print('\n# To deploy on a new EMR as Production Site')
            print('python %s -s production -N' % os.path.basename(__file__))
            print('\n# To change build on Beta Site')
            print('python %s -s beta -C' % os.path.basename(__file__))
            print('\n# To prepare testing build on current site')
            print('python %s -s test -b 280 -suffix eric_test -t 28800 -con 3 -m' % os.path.basename(__file__))
            print('\n# To check all Oozie job status')
            print('python %s -c all' % os.path.basename(__file__))
            print('\n# To check specific Oozie job status')
            print('python %s -c T1Security' % os.path.basename(__file__))
            print('\n# To check partitions for specific table by show partitions')
            print('python %s -p --database dp --table t_router_hourly --src list' % os.path.basename(__file__))
            print('\n# To check partitions for specific table by f_ flag')
            print('python %s -p --database dp --table t_router_hourly' % os.path.basename(__file__))
            print('\n# To check partitions for all table in sepcific database')
            print('python %s -p --database dp' % os.path.basename(__file__))
            print('\n# To check partitions for all database')
            print('python %s -p' % os.path.basename(__file__))
            print('\n# To repair partitions for specific table')
            print('python %s -r --database dp_beta --table t_device_hourly' % os.path.basename(__file__))
            print('\n# To repair partitions for all table in sepcific database')
            print('python %s -r --database dp' % os.path.basename(__file__))
            print('\n# To repair partitions for all database')
            print('python %s -r' % os.path.basename(__file__))

            exit(0)
        return parser.parse_args()


if __name__ == "__main__":
    DT = DeployTool()
    main_job = DT.command_parser()
    if main_job.site:
        if main_job.site not in ("production", "beta", "test"):
            print('Please assign site as "production", "beta" or "test".')
            exit()
        if main_job.site == "test":
            if main_job.build_name:
                build_folder, build_version = DT.get_build(build_name=main_job.build_name, mode="test")
            else:
                build_folder, build_version = DT.get_build(mode="test")
            DT.config_env(main_job.site, build_folder, build_version, suffix=main_job.suffix,
                          concurrency=main_job.concurrency, timeout=main_job.timeout, memory=main_job.memory)
            print('Testing build %s is ready to go' % build_version)
            print('Need to create database metadata')
            print('Need to msck repair')
            print('Need to set oozie jobs start and end time')
            DT.disable_stunnel()
        else:
            if main_job.new_deploy:
                build_folder, build_version = DT.get_build()
                DT.add_cronjob(main_job.site, build_folder)
                DT.config_env(main_job.site, build_folder, build_version)
                all_jobs, flag_list = DT.get_job_list_from_build(main_job.site, build_folder)
                DT.set_job_time(main_job.site, build_folder, all_jobs, flag_list)
                DT.deploy_build(main_job.site, build_folder)
            elif main_job.change_build:
                build_folder, build_version = DT.get_build()
                DT.config_env(main_job.site, build_folder, build_version)
                all_jobs, flag_list = DT.get_job_list_from_build(main_job.site, build_folder)
                previous_jobs = DT.wait_and_suspend_all_jobs(DT.get_job_info("all"))
                DT.set_job_time(main_job.site, build_folder, all_jobs, flag_list)
                DT.deploy_build(main_job.site, build_folder, suspend_jobs=previous_jobs, change_build=True)
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
        DT.check_job_status(main_job.check_job, DT.get_job_info(main_job.check_job))
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
    else:
        print('Please using -s <site>, -c <job>, -p or -r')
