import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from optparse import OptionParser


class DeployTool(object):
    AWS_PROD_S3_PATH = "s3://trs-production-us-west-2"
    AWS_BETA_S3_PATH = "s3://trs-production-beta-data-us-west-2"
    AWS_BUILD_PATH = "s3://eric-staging-us-west-2/build"
    AWS_SIGNATURE_PATH = "s3://eric-staging-us-west-2/signature"
    PROD_ENV_PATH = "output/data-pipeline-aws/op-utils/env"
    BETA_ENV_PATH = "output/data-pipeline-aws-beta/op-utils/env"
    BETA_OOZIE_PATH = "output/data-pipeline-aws-beta/oozie/*/job.properties"
    BETA_SCRIPT_PATH = "output/data-pipeline-aws-beta/script/hql_external_partition.sh"
    BETA_HQL_PATH = "output/data-pipeline-aws-beta/hql/*.hql"
    BETA_JOB_LIST = ["System-JobLogBackupDaily", "System-StunnelLogBackupDaily", "T0RouterInfo001Parquet",
                     "T0RouterStat001Parquet", "T0TmisCam001Parquet", "T1CamBfld", "T1CamInfo", "T1CamRule",
                     "T1CamSecurity", "T1CamSession", "T1CamStat", "T1CamTrs", "T1Device", "T1DeviceSession",
                     "T1Router", "T1RouterSecurity", "T1Rule", "T1Security", "T1Traffic", "T2CamCollectionDaily",
                     "T2CamCollectionWeekly", "T2CamIpsRuleHitCollection", "T2DeviceCollection", "T2DeviceStats",
                     "T2IpsRuleHitCollection", "T2RouterCollection", "T2RuleStats", "T2TrafficStats"]
    HOURLY_JOB = ["hourly", ["T0DatalakeAkamaiRgom", "T0DatalakeAkamaiWeb", "T0Ddi001Parquet", "T0Ncie001Parquet",
                             "T0RouterInfo001Parquet", "T0RouterStat001Parquet", "T0TmisCam001Parquet", "T1CamBfld",
                             "T1CamInfo", "T1CamSecurity", "T1CamSession", "T1CamStat", "T1CamTrs", "T1Device",
                             "T1DeviceSession", "T1Router", "T1RouterSecurity", "T1Security",
                             "TxExportAkamaiMalicious20171218", "TxExportDdi20171218", "TxExportIps20171218",
                             "TxExportNcie20171218", "TxExportRouterSecurity20171218", "TxPmSrcIps"]]
    DAILY_JOB = ["daily", ["System-JobLogBackupDaily", "System-StunnelLogBackupDaily", "T1CamRule", "T1Rule",
                           "T1Traffic", "T2CamCollectionDaily", "T2CamIpsRuleHitCollection", "T2DeviceCollection",
                           "T2DeviceStats", "T2IpsRuleHitCollection", "T2TrafficStats", "TxPmSrcIpsStats180d",
                           "TxPmSrcIpsStats1d", "TxPmSrcIpsStats30d", "TxPmSrcIpsStats7d", "TxPmSrcIpsStats90d"]]
    WEEKLY_JOB = ["weekly", ["T2CamCollectionWeekly", "T2RouterCollection", "T2RuleStats", "TxPmSrcDpiConfigStatsBrand",
                             "TxPmSrcDpiConfigStatsCountry", "TxPmSrcDpiConfigStatsRaw"]]
    PROD_MAPPING = {'T0DatalakeAkamaiRgom': 'Application/shnprj_spn/hive/datalake.db/f_akamai_rgom',
                    'T0DatalakeAkamaiWeb': 'Application/shnprj_spn/hive/datalake.db/f_akamai_web',
                    'T0Ddi001Parquet': 'Application/shnprj_spn/hive/dp.db/f_ddi_hourly',
                    'T0Ncie001Parquet': 'Application/shnprj_spn/hive/dp.db/f_ncie_hourly',
                    'T0RouterInfo001Parquet': 'Application/shnprj_spn/hive/dp.db/f_routerinfo_hourly',
                    'T0RouterStat001Parquet': 'Application/shnprj_spn/hive/dp.db/f_routerstat_hourly',
                    'T0TmisCam001Parquet': 'Application/shnprj_spn/hive/dp.db/f_tmis_cam_hourly',
                    'T1CamBfld': 'Application/shnprj_spn/hive/dp.db/f_cam_bfld_hourly',
                    'T1CamInfo': 'Application/shnprj_spn/hive/dp.db/f_cam_info_hourly',
                    'T1CamSecurity': 'Application/shnprj_spn/hive/dp.db/f_cam_security_hourly',
                    'T1CamSession': 'Application/shnprj_spn/hive/dp.db/f_cam_session_hourly',
                    'T1CamStat': 'Application/shnprj_spn/hive/dp.db/f_cam_stat_hourly',
                    'T1CamTrs': 'Application/shnprj_spn/hive/dp.db/f_cam_trs_hourly',
                    'T1Device': 'Application/shnprj_spn/hive/dp.db/f_device_hourly',
                    'T1DeviceSession': 'Application/shnprj_spn/hive/dp.db/f_device_session_hourly',
                    'T1Router': 'Application/shnprj_spn/hive/dp.db/f_router_hourly',
                    'T1RouterSecurity': 'Application/shnprj_spn/hive/dp.db/f_router_security_hourly',
                    'T1Security': 'Application/shnprj_spn/hive/dp.db/f_security_hourly',
                    'TxExportAkamaiMalicious20171218': 'trs_src/f_akamai_malicious_20171218',
                    'TxExportDdi20171218': 'trs_src/f_ddi_001_20171218',
                    'TxExportIps20171218': 'trs_src/f_ips_20171218',
                    'TxExportNcie20171218': 'trs_src/f_ncie_001_20171218',
                    'TxExportRouterSecurity20171218': 'trs_src/f_router_security_20171218',
                    'TxPmSrcIps': 'Application/shnprj_spn/hive/pm_src.db/f_ips_hourly',
                    'System-JobLogBackupDaily': '',
                    'System-StunnelLogBackupDaily': '',
                    'T1CamRule': 'Application/shnprj_spn/hive/dp.db/f_cam_rule_daily',
                    'T1Rule': 'Application/shnprj_spn/hive/dp.db/f_rule_daily',
                    'T1Traffic': 'Application/shnprj_spn/hive/dp.db/f_traffic_daily',
                    'T2CamCollectionDaily': 'Application/shnprj_spn/hive/dp.db/f_cam_collection_daily',
                    'T2CamIpsRuleHitCollection':
                        'Application/shnprj_spn/hive/dp.db/f_cam_ips_hit_rule_collection_daily',
                    'T2DeviceCollection': 'Application/shnprj_spn/hive/dp.db/f_device_collection_daily',
                    'T2DeviceStats': 'Application/shnprj_spn/hive/dp.db/f_router_device_daily',
                    'T2IpsRuleHitCollection': 'Application/shnprj_spn/hive/dp.db/f_ips_hit_rule_collection_daily',
                    'T2TrafficStats': 'Application/shnprj_spn/hive/dp.db/f_traffic_stats_daily',
                    'TxPmSrcIpsStats180d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=180d',
                    'TxPmSrcIpsStats1d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=1d',
                    'TxPmSrcIpsStats30d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=30d',
                    'TxPmSrcIpsStats7d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=7d',
                    'TxPmSrcIpsStats90d': 'Application/shnprj_spn/hive/pm_src.db/f_ips_stat_daily/period=90d',
                    'T2CamCollectionWeekly': 'Application/shnprj_spn/hive/dp.db/f_cam_collection_weekly',
                    'T2RouterCollection': 'Application/shnprj_spn/hive/dp.db/f_router_collection_weekly',
                    'T2RuleStats': 'Application/shnprj_spn/hive/dp.db/f_rule_stats_weekly',
                    'TxPmSrcDpiConfigStatsBrand':
                        'Application/shnprj_spn/hive/pm_src.db/f_dpi_config_stats_by_brand_weekly',
                    'TxPmSrcDpiConfigStatsCountry':
                        'Application/shnprj_spn/hive/pm_src.db/f_dpi_config_stats_by_country_weekly',
                    'TxPmSrcDpiConfigStatsRaw': 'Application/shnprj_spn/hive/pm_src.db/f_dpi_config_stats_raw_weekly'
                    }
    BETA_MAPPING = {'T0Ddi001Parquet': 'Application/shnprj_spn/hive/dp_beta.db/f_ddi_hourly',
                    'T0Ncie001Parquet': 'Application/shnprj_spn/hive/dp_beta.db/f_ncie_hourly',
                    'T0RouterInfo001Parquet': 'Application/shnprj_spn/hive/dp_beta.db/f_routerinfo_hourly',
                    'T0RouterStat001Parquet': 'Application/shnprj_spn/hive/dp_beta.db/f_routerstat_hourly',
                    'T0TmisCam001Parquet': 'Application/shnprj_spn/hive/dp_beta.db/f_tmis_cam_hourly',
                    'T1CamBfld': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_bfld_hourly',
                    'T1CamInfo': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_info_hourly',
                    'T1CamSecurity': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_security_hourly',
                    'T1CamSession': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_session_hourly',
                    'T1CamStat': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_stat_hourly',
                    'T1CamTrs': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_trs_hourly',
                    'T1Device': 'Application/shnprj_spn/hive/dp_beta.db/f_device_hourly',
                    'T1DeviceSession': 'Application/shnprj_spn/hive/dp_beta.db/f_device_session_hourly',
                    'T1Router': 'Application/shnprj_spn/hive/dp_beta.db/f_router_hourly',
                    'T1RouterSecurity': 'Application/shnprj_spn/hive/dp_beta.db/f_router_security_hourly',
                    'T1Security': 'Application/shnprj_spn/hive/dp_beta.db/f_security_hourly',
                    'System-JobLogBackupDaily': '',
                    'System-StunnelLogBackupDaily': '',
                    'T1CamRule': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_rule_daily',
                    'T1Rule': 'Application/shnprj_spn/hive/dp_beta.db/f_rule_daily',
                    'T1Traffic': 'Application/shnprj_spn/hive/dp_beta.db/f_traffic_daily',
                    'T2CamCollectionDaily': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_collection_daily',
                    'T2CamIpsRuleHitCollection':
                        'Application/shnprj_spn/hive/dp_beta.db/f_cam_ips_hit_rule_collection_daily',
                    'T2DeviceCollection': 'Application/shnprj_spn/hive/dp_beta.db/f_device_collection_daily',
                    'T2DeviceStats': 'Application/shnprj_spn/hive/dp_beta.db/f_router_device_daily',
                    'T2IpsRuleHitCollection': 'Application/shnprj_spn/hive/dp_beta.db/f_ips_hit_rule_collection_daily',
                    'T2TrafficStats': 'Application/shnprj_spn/hive/dp_beta.db/f_traffic_stats_daily',
                    'T2CamCollectionWeekly': 'Application/shnprj_spn/hive/dp_beta.db/f_cam_collection_weekly',
                    'T2RouterCollection': 'Application/shnprj_spn/hive/dp_beta.db/f_router_collection_weekly',
                    'T2RuleStats': 'Application/shnprj_spn/hive/dp_beta.db/f_rule_stats_weekly'}
    VERSION = "20180416"

    def __init__(self):
        self.build_folder = ""
        self.build_version = ""
        self.previous_jobs = {}

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

    def get_build(self):
        build_file = self.run_command("aws s3 ls %s/ | grep 'SHN-Data-Pipeline' | sort | tail -1 | awk '{print $4}'"
                                      % self.AWS_BUILD_PATH)[:-1]
        if not build_file:
            raise Exception('[Error] No available build to deploy')
        self.run_command("aws s3 cp %s/%s /home/hadoop/" % (self.AWS_BUILD_PATH, build_file))
        self.run_command("tar -C /home/hadoop/ -zxvf /home/hadoop/%s" % build_file)
        self.build_folder = "/home/hadoop/%s" % build_file.split('.tar')[0]
        self.build_version = self.build_folder.split('1.0.')[1]

    def config_env(self, site):
        if site == "production":
            self.run_command("cp %s/%s/aws-production.sh %s/%s/$(whoami)\@$(hostname).sh"
                             % (self.build_folder, self.PROD_ENV_PATH, self.build_folder, self.PROD_ENV_PATH))
            self.run_command("echo 'OOZIE_APP_EXT=.AWS_Production%s' >> %s/%s/$(whoami)\@$(hostname).sh" %
                             (self.build_version, self.build_folder, self.PROD_ENV_PATH))
        elif site == "beta":
            self.run_command("cp %s/%s/aws-production-beta-data.sh %s/%s/$(whoami)\@$(hostname).sh"
                             % (self.build_folder, self.BETA_ENV_PATH, self.build_folder, self.BETA_ENV_PATH))
            self.run_command("echo 'OOZIE_APP_EXT=.AWS_Beta%s' >> %s/%s/$(whoami)\@$(hostname).sh" %
                             (self.build_version, self.build_folder, self.BETA_ENV_PATH))
            self.run_command("sed -i 's/^cntLowerbound=.*$/cntLowerbound=0/g' %s/%s" %
                             (self.build_folder, self.BETA_OOZIE_PATH))
            self.run_command("sed -i 's/ --driver-memory 12G --executor-memory 12G//g' %s/%s" %
                             (self.build_folder, self.BETA_SCRIPT_PATH))
            self.run_command("sed -i '/SET hive.tez.java.opts=-Xmx10240m;/d' %s/%s" %
                             (self.build_folder, self.BETA_HQL_PATH))
        else:
            pass

    def set_job_time(self, site):
        job_time_list = []
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
            mapping = self.PROD_MAPPING
        else:
            site_s3_path = self.AWS_BETA_S3_PATH
            output_path = "data-pipeline-aws-beta"
            mapping = self.BETA_MAPPING

        if jobs[0] == "hourly":
            add_time = timedelta(hours=2)
        elif jobs[0] == "daily":
            add_time = timedelta(days=2)
        else:
            add_time = timedelta(days=8)

        for job in jobs[1]:
            if site == "beta" and job not in self.BETA_JOB_LIST:
                continue

            # get minutes from app-time.conf
            f_flag_minute = self.run_command(
                "cat %s/output/%s/op-utils/app-time.conf | grep '%s' | grep coordStart | head -1 " % (
                    self.build_folder, output_path, job))[-4:-1]
            if not re.match('\d{2}Z', f_flag_minute):
                raise Exception('[Error] Get malformed minute from app-time.conf:', f_flag_minute)

            # get datetime from aws
            if "System" in job:
                f_flag_day = datetime.now().strftime('%Y-%m-%d')
            else:
                f_flag_day = self.run_command("aws s3 ls %s/%s/ | tail -1 | awk '{print $4}' | cut -d'_' -f1" %
                                              (site_s3_path, mapping[job]))[-11:-1]
            if not re.match('\d{4}-\d{2}-\d{2}', f_flag_day):
                raise Exception('[Error] Get malformed day from s3:', f_flag_day)

            # get hours from aws with datetime
            if jobs[0] == "hourly":
                if "TxExport" in job:
                    f_flag_hour = self.run_command("aws s3 ls %s/%s/pdd=%s/ | tail -1 | awk '{print $4}'" %
                                                   (site_s3_path, mapping[job], f_flag_day))[5:6]
                else:
                    f_flag_hour = self.run_command("aws s3 ls %s/%s/d=%s/ | tail -1 | awk '{print $4}'" %
                                                   (site_s3_path, mapping[job], f_flag_day))[3:4]
                if not re.match('\d{1,2}', f_flag_hour):
                    raise Exception('[Error] Get malformed hour from s3:', f_flag_hour)
            elif "System" in job:
                f_flag_hour = "02"
            else:
                f_flag_hour = "00"

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
            self.run_command("bash %s/deploy.sh all" % deploy_folder)
        except Exception:
            print('[Error] Oozie dry run failed')
            exit()
        else:
            if change_build:
                self.kill_all_job(self.get_job_info("all"))
        if site == "production":
            self.run_command("sed -i '/DeviceSession/d' %s/run-jobs.sh" % deploy_folder)

        #   to be change as run_command
        self.run_command("bash %s/run-jobs.sh" % deploy_folder)
        #   print("bash %s/run-jobs.sh" % deploy_folder)

    def kill_all_job(self, oozie_job_list):
        print('=== Kill All Job (Count: %s) ===' % len(oozie_job_list))
        for job in oozie_job_list:
            self.run_command("oozie job -kill %s" % oozie_job_list[job][0])
            #   print("oozie job -kill %s" % oozie_job_list[job][0])

    def suspend_all_job(self, oozie_job_list):
        print('=== Suspend All Job ===')
        for job in oozie_job_list:
            self.run_command("oozie job -suspend %s" % oozie_job_list[job][0])
            #   print("oozie job -suspend %s" % oozie_job_list[job][0])

    def resume_all_job(self, oozie_job_list):
        print('=== Resume All Job ===')
        for job in oozie_job_list:
            self.run_command("oozie job -resume %s" % oozie_job_list[job][0])
            #   print("oozie job -resume %s" % oozie_job_list[job][0])

    def get_job_info(self, job):
        print('\nCurrent status of Oozie job:')
        if job == "all":
            job = ""
        info = self.run_command(
            "oozie jobs info -jobtype coordinator -len 3000|grep '%s.*RUNNING\|%s.*PREP'|sort -k8" % (job, job),
            show_command=False)[:-1].rstrip('\n').split('\n')
        print("JobID\t\t\t\t     Next Materialized    App Name")
        app_info = {}
        for each in info:
            result = re.findall('(.*-oozie-oozi-C)[ ]*(%s.*)\.[\S ]*.*GMT    ([0-9: -]*).*    ' % job, each)
            if len(result) > 0:
                print(result[0][0], result[0][2], result[0][1])
                app_info.update({result[0][1]: [result[0][0], result[0][2]]})
        print('Total jobs: %s' % len(app_info))
        print('\nCurrent time: %s' % datetime.now())
        return app_info

    def check_job_status(self, job, oozie_job_list):
        jobs_to_hide = '\|SUCCEEDED\|READY'
        if job == "all":
            counter = 1
            for app in oozie_job_list:
                print('\n=== Job Checking(%d/%d) ===' % (counter, len(oozie_job_list)))
                print(self.run_command("oozie job -info %s |grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" % (
                    oozie_job_list[app][0], jobs_to_hide), show_command=False))
                counter += 1
        else:
            if job in oozie_job_list:
                print('=== Job Checking ===')
                print(self.run_command("oozie job -info %s |grep -v '\-\-\|Pause Time\|App Path\|Job ID%s'" % (
                    oozie_job_list[job][0], jobs_to_hide), show_command=False))
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

    @staticmethod
    def command_parser():
        usage = "\t%s [options]\nTool version:\t%s" % (sys.argv[0], "20180222")
        parser = OptionParser(usage)
        parser.add_option("-s", type="string", dest="site", help='Choose deploy target site')
        parser.add_option("-N", action="store_true", dest="new_deploy", help='Execute a new deploy on EMR')
        parser.add_option("-C", action="store_true", dest="change_build", help='Execute change build on EMR')
        parser.add_option("-c", type="string", dest="check_job", help='Check Oozie job status')

        if len(sys.argv) == 1:
            parser.print_help()
            print('\nQuick Start:')
            #  add where build come from
            print('# To deploy on a new EMR as Production Site')
            print('python %s -s production -N' % os.path.basename(__file__))
            print('# To deploy on a new EMR as Beta Site')
            print('python %s -s beta -N' % os.path.basename(__file__))
            print('# To change build on Production Site')
            print('python %s -s production -C' % os.path.basename(__file__))
            print('# To change build on Production Site')
            print('python %s -s beta -C' % os.path.basename(__file__))
            print('# To check all Oozie job status')
            print('python %s -c "All"' % os.path.basename(__file__))
            print('# To check specific Oozie job status')
            print('python %s -c "T1Security"' % os.path.basename(__file__))
            exit()
        return parser.parse_args()[0]


if __name__ == "__main__":
    DT = DeployTool()
    main_job = DeployTool.command_parser()
    if main_job.site:
        if main_job.site != "production" and main_job.site != "beta":
            print('Please assign site as "production" or "beta.".')
        else:
            if main_job.change_build and main_job.new_deploy:
                print('Please choose one option for new deploy(-n)/change build(-c).')
            elif main_job.new_deploy:
                DT.get_build()
                DT.add_cronjob(main_job.site)
                DT.config_env(main_job.site)
                DT.set_job_time(main_job.site)
                DT.deploy(main_job.site)
            elif main_job.change_build:
                DT.get_build()
                DT.config_env(main_job.site)
                DT.set_job_time(main_job.site)
                DT.deploy(main_job.site, change_build=True)
    elif main_job.check_job:
        DT.check_job_status(main_job.check_job, DT.get_job_info(main_job.check_job))
