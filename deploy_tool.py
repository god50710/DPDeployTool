import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from optparse import OptionParser


class DeployTool(object):
    AWS_PROD_S3_PATH = "s3://trs-production-us-west-2"
    AWS_BETA_S3_PATH = "s3://trs-production-beta-data-us-west-2"
    AWS_VERIFIED_BUILD_PATH = "s3://eric-staging-us-west-2/build"
    AWS_TESTING_BUILD_PATH = "s3://eric-staging-us-west-2/test_build"
    AWS_SIGNATURE_PATH = "s3://eric-staging-us-west-2/signature"
    VERSION = "20180425"
    REPAIR_TABLES = {
        "trs_src": ["akamai_malicious_20171218",
                    "akamai_malicious_20180319",
                    "ddi_001_20171218",
                    "honeypot_ssh_2",
                    "honeypot_telnet_2",
                    "ips_20171218",
                    "ncie_001_20171218",
                    "router_security_20171218"],
        "pm_src": ["t_dpi_config_stats_by_brand_weekly",
                   "t_dpi_config_stats_by_country_weekly",
                   "t_dpi_config_stats_raw_weekly",
                   "t_ips_hourly",
                   "t_ips_stat_daily"],
        "dp": ["e_app_inf",
               "e_app_rule",
               "e_country_region_mapping",
               "e_ddi_001_parquet",
               "e_device_family",
               "e_device_name",
               "e_device_type",
               "e_fingerprint",
               "e_ips_rule",
               "e_ips_rule_bypass_list",
               "e_ncie_001_parquet",
               "e_os_class",
               "e_os_name",
               "e_os_vendor",
               "e_routerinfo_001_parquet",
               "e_routerstat_001_parquet",
               "e_tmis_cam_001_parquet",
               "e_user_agent",
               "t_cam_bfld_hourly",
               "t_cam_collection_daily",
               "t_cam_collection_weekly",
               "t_cam_info_hourly",
               "t_cam_ips_hit_rule_collection_daily",
               "t_cam_rule_daily",
               "t_cam_security_hourly",
               "t_cam_session_hourly",
               "t_cam_stat_hourly",
               "t_cam_trs_hourly",
               "t_device_collection_daily",
               "t_device_hourly",
               "t_device_session_hourly",
               "t_ips_hit_rule_collection_daily",
               "t_router_collection_weekly",
               "t_router_device_daily",
               "t_router_hourly",
               "t_router_scanned_port_event_aggregation_daily",
               "t_router_security_hourly",
               "t_rule_daily",
               "t_rule_stats_weekly",
               "t_security_hourly",
               "t_traffic_daily",
               "t_traffic_stats_daily",
               ],
        "datalake": ["akamai_rgom",
                     "akamai_web",
                     "iotlog"]}

    def __init__(self):
        self.build_folder = ""
        self.build_version = ""
        self.previous_jobs = {}
        self.HOURLY_JOB = ["hourly"]
        self.DAILY_JOB = ["daily"]
        self.WEEKLY_JOB = ["weekly"]
        self.FLAG_MAPPING = {}

    @staticmethod
    def run_command(cmd, show_command=True, throw_error=True):
        if show_command:
            print(cmd)
        o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = o.communicate()
        if result[1] != "":
            print(result)
            if throw_error:
                raise Exception('[Error] Run command returns stderr')
        else:
            return result[0]

    def get_mapping_list(self, site):
        output_path = "output/data-pipeline-aws"
        if site == "beta":
            output_path = output_path + "-beta"
        output_path = self.build_folder + "/" + output_path

        hourly_job = []
        daily_job = []
        weekly_job = []
        flag = ""
        # system monitor jobs no f_flag, add to mapping list with '' directly
        system_jobs = self.run_command("ls -d %s/oozie/System*" % output_path).split()
        for job in system_jobs:
            job = job.split('/')[-1]
            self.FLAG_MAPPING[job] = ''
            daily_job.append(job)

        table_jobs = self.run_command("ls -d %s/oozie/T*" % output_path).split()
        for job_path in table_jobs:
            job = job_path.split('/')[-1]
            frequency = self.run_command("grep 'coordExecFreq=' %s/job.properties | tail -n 1" % job_path)
            data_out = self.run_command("grep 'dataOut=' %s/job.properties | tail -n 1 | cut -d'=' -f2" %
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

            flag_path_prefix = self.run_command("grep '%s' %s | grep 'uri-template'" %
                                                (flag, dataset_path)).split('${nameNode}/')[1].split('${')[0]
            if database != "trs_src":
                if site == "beta":
                    database = database + "_beta.db"
                else:
                    database = database + ".db"

            self.FLAG_MAPPING[job] = '%s%s/%s' % (flag_path_prefix, database, flag)
            if "hours(1)" in frequency:
                hourly_job.append(job)
            elif "days(1)" in frequency:
                daily_job.append(job)
            elif "days(7)" in frequency:
                weekly_job.append(job)
            else:
                raise Exception('[Error] frequency out of excepted: %s' % frequency)
        self.HOURLY_JOB.append(hourly_job)
        self.DAILY_JOB.append(daily_job)
        self.WEEKLY_JOB.append(weekly_job)

    def get_build(self,build_name=""):
        if not build_name:
            build_folder = self.AWS_VERIFIED_BUILD_PATH
            build_file = self.run_command("aws s3 ls %s/ | grep 'SHN-Data-Pipeline' | sort | tail -1 | awk '{print $4}'"
                                          % self.AWS_VERIFIED_BUILD_PATH)[:-1]
        else:
            build_folder = self.AWS_TESTING_BUILD_PATH
            build_file = self.run_command(
                "aws s3 ls %s/ | grep 'SHN-Data-Pipeline' | grep '%s' | sort | tail -1 | awk '{print $4}'"
                    % (build_folder, build_name))[:-1]
        if not build_file:
            raise Exception('[Error] No available build to deploy')
        self.run_command("aws s3 cp %s/%s /home/hadoop/" % (build_folder, build_file))
        self.run_command("tar -C /home/hadoop/ -zxvf /home/hadoop/%s" % build_file)
        self.build_folder = "/home/hadoop/%s" % build_file.split('.tar')[0]
        self.build_version = self.build_folder.split('1.0.')[1]

    def config_env(self, site, suffix="function", concurrency=1, timeout=180, memory=True):
        if site == "production":
            prod_env_path = "%s/output/data-pipeline-aws/op-utils/env" % self.build_folder
            self.run_command("cp %s/aws-production.sh %s/$(whoami)\@$(hostname).sh" %
                             (prod_env_path, prod_env_path))
            self.run_command("echo 'OOZIE_APP_EXT=.AWS_Production%s' >> %s/$(whoami)\@$(hostname).sh" %
                             (self.build_version, prod_env_path))
        elif site == "beta":
            beta_env_path = "%s/output/data-pipeline-aws-beta/op-utils/env" % self.build_folder
            beta_oozie_jobs_path = "%s/output/data-pipeline-aws-beta/oozie/*/job.properties" % self.build_folder
            beta_script_path = "%s/output/data-pipeline-aws-beta/script/hql_external_partition.sh" % self.build_folder
            beta_hql_path = "%s/output/data-pipeline-aws-beta/hql/*.hql" % self.build_folder
            self.run_command(
                "cp %s/aws-production-beta-data.sh %s/$(whoami)\@$(hostname).sh" % (beta_env_path, beta_env_path))
            self.run_command("echo 'OOZIE_APP_EXT=.AWS_Beta%s' >> %s/$(whoami)\@$(hostname).sh" %
                             (self.build_version, beta_env_path))
            self.run_command("sed -i 's/^cntLowerbound=.*$/cntLowerbound=0/g' %s" % beta_oozie_jobs_path)
            self.run_command("sed -i 's/ --driver-memory 12G --executor-memory 12G//g' %s" % beta_script_path)
            self.run_command("sed -i '/SET hive.tez.java.opts=-Xmx10240m;/d' %s" % beta_hql_path)
            self.run_command("sed -i '/SET hive.tez.container.size=12288;/d' %s" % beta_hql_path)
        elif site == "test":
            test_env_path = "%s/output/data-pipeline-aws/op-utils/env" % self.build_folder
            test_oozie_path = "%s/output/data-pipeline-aws/oozie" % self.build_folder
            test_script_path = "%s/output/data-pipeline-aws/script/hql_external_partition.sh" % self.build_folder
            test_hql_path = "%s/output/data-pipeline-aws/hql/*.hql" % self.build_folder
            test_env_make_path = "%s/src" % self.build_folder
            self.run_command("cd %s; make clean" % test_env_make_path)
            if timeout != 180:
                self.run_command("cd %s; sed -i 's/180/%s/g' data-pipeline/oozie/common.properties" %
                                 (test_env_make_path, timeout))
            self.run_command("cd %s; make %s-db" % (test_env_make_path, suffix))
            if not memory:
                self.run_command("sed -i 's/ --driver-memory 12G --executor-memory 12G//g' %s" % test_script_path)
                self.run_command("sed -i '/SET hive.tez.java.opts=-Xmx10240m;/d' %s" % test_hql_path)
                self.run_command("sed -i '/SET hive.tez.container.size=12288;/d' %s" % test_hql_path)

            self.run_command("sed -i 's/concurrency=./concurrency=%i/g' %s/*/job.properties" %
                             (concurrency, test_oozie_path))
            self.run_command("cp %s/hadoop\@ip-172-31-13-117.sh %s/$(whoami)\@$(hostname).sh" %
                             (test_env_path, test_env_path))
            self.run_command("sed -i '/HADOOP_NAME_NODE/d' %s/$(whoami)\@$(hostname).sh" % test_env_path)
            self.run_command("echo 'export HADOOP_NAME_NODE=s3://dp-%s' >> %s/$(whoami)\@$(hostname).sh" %
                             (suffix, test_env_path))
            self.run_command("echo 'OOZIE_APP_EXT=.AWS_Test%s' >> %s/$(whoami)\@$(hostname).sh" %
                             (self.build_version, test_env_path))

    def set_job_time(self, site):
        job_time_list = list()
        job_time_list.append("#hourly jobs")
        job_time_list.extend(self.scan_f_flag(site, self.HOURLY_JOB))
        job_time_list.append("#daily jobs")
        job_time_list.extend(self.scan_f_flag(site, self.DAILY_JOB))
        job_time_list.append("#weekly jobs")
        job_time_list.extend(self.scan_f_flag(site, self.WEEKLY_JOB))
        self.export_app_time(site, job_time_list)

    def export_app_time(self, site, job_time_list):
        if site == "production":
            output_path = "data-pipeline-aws"
        else:
            output_path = "data-pipeline-aws-beta"
        deploy_folder = "%s/output/%s/op-utils" % (self.build_folder, output_path)
        job_time_file = open("%s/app-time.conf" % deploy_folder, "w")
        for line in job_time_list:
            job_time_file.write(line + "\n")
        job_time_file.close()

    def scan_f_flag(self, site, jobs):
        job_time_list = []
        if site == "production":
            site_s3_path = self.AWS_PROD_S3_PATH
            output_path = "data-pipeline-aws"
        else:
            site_s3_path = self.AWS_BETA_S3_PATH
            output_path = "data-pipeline-aws-beta"

        if jobs[0] == "hourly":
            add_time = timedelta(hours=2)
        elif jobs[0] == "daily":
            add_time = timedelta(days=2)
        else:
            add_time = timedelta(days=8)

        for job in jobs[1]:
            # get minutes from app-time.conf
            f_flag_minute = self.run_command(
                "cat %s/output/%s/op-utils/app-time.conf | grep '%s' | grep coordStart | head -1 " % (
                    self.build_folder, output_path, job))[-4:-1]
            if not re.match('\d{2}Z', f_flag_minute):
                raise Exception('[Error] Get malformed minute from app-time.conf:', f_flag_minute)

            # get datetime from aws
            if "System" in job:
                f_flag_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                f_flag_day = self.run_command("aws s3 ls %s/%s/ | tail -1 | awk '{print $4}' | cut -d'_' -f1" %
                                              (site_s3_path, self.FLAG_MAPPING[job]))[-11:-1]
            if not re.match('\d{4}-\d{2}-\d{2}', f_flag_day):
                raise Exception('[Error] Get malformed day from s3:', f_flag_day)

            # get hours from aws with datetime
            if jobs[0] == "hourly":
                if "TxExport" in job:
                    f_flag_hour = self.run_command("aws s3 ls %s/%s/pdd=%s/ | tail -1 | awk '{print $4}'" %
                                                   (site_s3_path, self.FLAG_MAPPING[job], f_flag_day))[4:6]
                else:
                    f_flag_hour = self.run_command("aws s3 ls %s/%s/d=%s/ | tail -1 | awk '{print $4}'" %
                                                   (site_s3_path, self.FLAG_MAPPING[job], f_flag_day))[2:4]
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

    def deploy(self, site, change_build=False):
        if site == "production":
            output_path = "data-pipeline-aws"
        else:
            output_path = "data-pipeline-aws-beta"
        deploy_folder = "%s/output/%s/op-utils" % (self.build_folder, output_path)
        try:
            self.run_command("bash %s/deploy.sh all" % deploy_folder, throw_error=False)
            if site == "production":
                self.run_command("sed -i '/DeviceSession/d' %s/run-jobs.sh" % deploy_folder)
            #  self.run_command("bash %s/run-jobs.sh" % deploy_folder)
            print("bash %s/run-jobs.sh" % deploy_folder)
        except Exception:
            print('[Error] Deploy failed, resume previous jobs')
            if change_build:
                self.resume_all_job(self.previous_jobs)
            exit(1)
        else:
            if change_build:
                self.kill_all_job(self.previous_jobs)

    def wait_and_suspend_all_jobs(self, oozie_job_list):
        counter = 1
        self.previous_jobs = oozie_job_list
        while True:
            for oozie_job in oozie_job_list:
                cannot_suspend_job = self.run_command(
                    "oozie job -info %s | grep oozie-oozi-C@ | grep 'RUNNING\|SUSPENDED'" %
                    (oozie_job_list[oozie_job][0]), show_command=False)
                cannot_suspend_status = self.run_command("oozie job -info %s | grep 'Status' | grep 'SUSPENDED'" %
                                                         (oozie_job_list[oozie_job][0]), show_command=False)
                if not (cannot_suspend_job or cannot_suspend_status):
                    print('=== Suspending %s (%d/%d) ===' % (oozie_job, counter, len(oozie_job_list)))
                    self.run_command("oozie job -suspend %s" % oozie_job_list[oozie_job][0])
                    counter += 1
                if counter > len(oozie_job_list):
                    break
            if counter > len(oozie_job_list):
                break

    def kill_all_job(self, oozie_job_list):
        print('=== Kill All Jobs (Count: %s) ===' % len(oozie_job_list))
        for oozie_job in oozie_job_list:
            self.run_command("oozie job -kill %s" % oozie_job_list[oozie_job][0])
            #   print("oozie job -kill %s" % oozie_job_list[oozie_job][0])

    def suspend_all_job(self, oozie_job_list):
        print('=== Suspend All Jobs ===')
        for oozie_job in oozie_job_list:
            self.run_command("oozie job -suspend %s" % oozie_job_list[oozie_job][0])
            #   print("oozie job -suspend %s" % oozie_job_list[oozie_job][0])

    def resume_all_job(self, oozie_job_list):
        print('=== Resume All Jobs ===')
        for oozie_job in oozie_job_list:
            self.run_command("oozie job -resume %s" % oozie_job_list[oozie_job][0])
            #   print("oozie job -resume %s" % oozie_job_list[oozie_job][0])

    def get_job_info(self, target):
        print('\nCurrent status of Oozie job:')
        if target == "all":
            target = ""
        info = self.run_command(
            "oozie jobs info -jobtype coordinator -len 3000|grep '%s.*RUNNING\|%s.*PREP\|%s.*SUSPEND'|sort -k8" %
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

    def check_job_status(self, oozie_job, oozie_job_list):
        jobs_to_hide = '\|SUCCEEDED\|READY'
        if oozie_job == "all":
            counter = 1
            for oozie_job in oozie_job_list:
                print('\n=== Job Checking(%d/%d) ===' % (counter, len(oozie_job_list)))
                print(self.run_command("oozie job -info %s -len 3000|grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" %
                                       (oozie_job_list[oozie_job][0], jobs_to_hide), show_command=False))
                counter += 1
        else:
            if oozie_job in oozie_job_list:
                print('=== Job Checking ===')
                print(self.run_command("oozie job -info %s |grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" %
                                       (oozie_job_list[oozie_job][0], jobs_to_hide), show_command=False))
            else:
                print('Job not found in Oozie job list')

    def add_cronjob(self, site):
        cronjob_file = "/home/hadoop/cron_temp"
        self.run_command("crontab -l > %s" % cronjob_file)
        signature_cronjob = self.run_command("cat %s | grep 'update_signature/bg_executor.sh'" % cronjob_file)
        geoip_cronjob = self.run_command("cat %s | grep 'update_geoip/geoip_bg_executor.sh'" % cronjob_file)
        if not signature_cronjob:
            self.run_command("cp -r %s/QA/update_signature /home/hadoop/" % self.build_folder)
            self.run_command("echo '0 * * * * /home/hadoop/update_signature/bg_executor.sh %s' >> %s " %
                             (site, cronjob_file))
        if geoip_cronjob:
            self.run_command("sed -i '/geoip_bg_executor.sh/d' %s" % cronjob_file)
            self.run_command("echo '0 * * * * /trs/update_geoip/geoip_bg_executor_with_mail.sh %s' >> %s " %
                             (site, cronjob_file))
        self.run_command("crontab %s" % cronjob_file)
        self.run_command("rm %s" % cronjob_file)

    def disable_stunnel(self):
        self.run_command("ps -ef | grep stunnel | awk '{print $1}' | xargs sudo kill -9")

    def repair_partition(self, site, database):
        def repair(repair_site, repair_database):
            repair_tables = self.REPAIR_TABLES[repair_database]
            if repair_site == "beta":
                repair_database = repair_database + "_beta"
            for table in repair_tables:
                # self.run_command('beeline -u "jdbc:hive2://localhost:10000/" -e "msck repair table %s.%s;"' %
                # (repair_database, table))
                print('beeline -u "jdbc:hive2://localhost:10000/" -e "msck repair table %s.%s;"' %
                      (repair_database, table))

        if (site == "production" and database in ("trs_src", "pm_src", "datalake", "dp")) or (
                site == "beta" and database == "dp"):
            repair(site, database)
        elif database == "all":
            if site == "beta":
                repair(site, "dp")
            else:
                for database in self.REPAIR_TABLES.keys():
                    repair(site, database)
        else:
            raise Exception('[Error] Repair target database %s is invalid' % database)

    def command_parser(self):
        usage = "\t%s [options]\nTool version:\t%s" % (sys.argv[0], self.VERSION)
        parser = OptionParser(usage)
        parser.add_option("-s", type="string", dest="site", help="Choose deploy target site")
        parser.add_option("-N", action="store_true", dest="new_deploy", help="Execute a new deploy on EMR")
        parser.add_option("-C", action="store_true", dest="change_build", help="Execute change build on EMR")
        parser.add_option("-c", type="string", dest="check_job", help="Check Oozie job status")
        parser.add_option("-r", type="string", dest="repair", help="Repair partitions")
        parser.add_option("-b", type="string", dest="build_name", help="Specify build name")
        parser.add_option("-t", type="int", dest="timeout", default="180", help="Set oozie job timeout")
        parser.add_option("--suffix", type="string", dest="suffix", default="function",
                          help='Set database/s3 bucket name suffix')
        parser.add_option("--con", type="int", dest="concurrency", default=1, help="Set oozie jobs concurrency")
        parser.add_option("-m", action="store_true", dest="memory", default=False, help="Keep hql memory limits")
        if len(sys.argv) == 1:
            parser.print_help()
            print('\nQuick Start:')
            print('# Verified build location: %s' % self.AWS_VERIFIED_BUILD_PATH)
            print('# Testing build location: %s' % self.AWS_TESTING_BUILD_PATH)
            print('# To deploy on a new EMR as Production Site')
            print('python %s -s production -N' % os.path.basename(__file__))
            print('# To change build on Beta Site')
            print('python %s -s beta -C' % os.path.basename(__file__))
            print('# To repair partition on Production Site')
            print('python %s -s production -r' % os.path.basename(__file__))
            print('python %s -s beta -C' % os.path.basename(__file__))
            print('# To prepare testing build on current site')
            print('python %s -s test -b 280 -suffix eric_test -t 28800 -con 3 -m' %
                  os.path.basename(__file__))
            print('# To check all Oozie job status')
            print('python %s -c all' % os.path.basename(__file__))
            print('# To check specific Oozie job status')
            print('python %s -c T1Security' % os.path.basename(__file__))
            exit(0)
        return parser.parse_args()[0]


if __name__ == "__main__":
    DT = DeployTool()
    main_job = DT.command_parser()
    if main_job.site:
        if main_job.site not in ("production", "beta", "test"):
            print('Please assign site as "production", "beta" or "test".')
        else:
            if main_job.site == "test":
                if main_job.build_name:
                    DT.get_build(main_job.build_name)
                    DT.config_env(main_job.site, suffix=main_job.suffix, concurrency=main_job.concurrency,
                                  timeout=main_job.timeout, memory=main_job.memory)
                    print('Testing build %s is ready to go' % DT.build_version)
                    print('Need to create database metadata')
                    print('Need to msck repair')
                    print('Need to set oozie jobs start and end time')
                    print('Need to kill stunnel')
                elif main_job.repair:
                    print('Testing site cannot using repair partition function')

            else:
                if main_job.change_build and main_job.new_deploy:
                    print('Please choose one option for new deploy(-N)/change build(-C)/repair partition(-r).')
                elif main_job.new_deploy:
                    DT.get_build()
                    DT.add_cronjob(main_job.site)
                    DT.config_env(main_job.site)
                    DT.get_mapping_list(main_job.site)
                    DT.set_job_time(main_job.site)
                    DT.deploy(main_job.site)
                elif main_job.change_build:
                    DT.get_build()
                    DT.config_env(main_job.site)
                    DT.get_mapping_list(main_job.site)
                    DT.wait_and_suspend_all_jobs(DT.get_job_info("all"))
                    DT.set_job_time(main_job.site)
                    DT.deploy(main_job.site, change_build=True)
                elif main_job.repair:
                    DT.repair_partition(main_job.site, main_job.repair)
    elif main_job.check_job:
        DT.check_job_status(main_job.check_job, DT.get_job_info(main_job.check_job))
