import os
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
                           "T2IpsRuleHitCollection", "T2TrafficStats", "TxPmSrcIpsStats180d", "TxPmSrcIpsStats1d",
                           "TxPmSrcIpsStats30d", "TxPmSrcIpsStats7d", "TxPmSrcIpsStats90d"]]
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

    @staticmethod
    def run_command(cmd, show_command=True):
        if show_command:
            print(cmd)
        o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = o.communicate()
        if result[1] != "":
            print(result)
            raise Exception('Run command has stderr')
        else:
            return result[0]

    def get_build(self):
        build_file = self.run_command("aws s3 ls %s/ | grep 'SHN-Data-Pipeline' | sort | tail -1 | awk '{print $4}'"
                                      % self.AWS_BUILD_PATH)[:-1]
        if not build_file:
            raise Exception('No available build to deploy')
        self.run_command("aws s3 cp %s/%s /home/hadoop/" % (self.AWS_BUILD_PATH, build_file))
        self.run_command("tar -zxvf /home/hadoop/%s" % build_file)
        print("[Info] Build %s is ready" % build_file.split('.tar')[0])
        self.build_folder = "/home/hadoop/%s" % build_file.split('.tar')[0]
        self.build_version = self.build_folder.split('1.0.')[1]

    def config_env(self, site):
        if site == "production":
            self.run_command("cp %s/%s/aws-production.sh %s/%s/$(whoami)\@$(hostname).sh"
                             % (self.build_folder, self.PROD_ENV_PATH, self.build_folder, self.PROD_ENV_PATH))
            self.run_command("echo 'OOZIE_APP_EXT=.AWSProduction%s' >> %s/%s/$(whoami)\@$(hostname).sh" %
                             (self.build_version, self.build_folder, self.PROD_ENV_PATH))
        else:
            self.run_command("cp %s/%s/aws-production-beta-data.sh %s/%s/$(whoami)\@$(hostname).sh"
                             % (self.build_folder, self.BETA_ENV_PATH, self.build_folder, self.BETA_ENV_PATH))
            self.run_command("echo 'OOZIE_APP_EXT=.AWSBeta%s' >> %s/%s/$(whoami)\@$(hostname).sh" %
                             (self.build_version, self.build_folder, self.BETA_ENV_PATH))
            self.run_command("sed -i 's/^cntLowerbound=.*$/cntLowerbound=0/g' %s/%s" %
                             (self.build_folder, self.BETA_OOZIE_PATH))
            self.run_command("sed -i 's/ --driver-memory 12G --executor-memory 12G//g' %s/%s" %
                             (self.build_folder, self.BETA_SCRIPT_PATH))
            self.run_command("sed -i '/SET hive.tez.java.opts=-Xmx10240m;/d' %s/%s" %
                             (self.build_folder, self.BETA_HQL_PATH))

    def set_app_time(self, site):
        app_time_list = []
        app_time_list.append("#hourly jobs")
        app_time_list.extend(self.scan_f_flag(site, self.HOURLY_JOB))
        app_time_list.append("#daily jobs")
        app_time_list.extend(self.scan_f_flag(site, self.DAILY_JOB))
        app_time_list.append("#weekly jobs")
        app_time_list.extend(self.scan_f_flag(site, self.WEEKLY_JOB))
        self.export_app_time(site, app_time_list)

    @staticmethod
    def export_app_time(site, app_time_list):
        if site == "production":
            output_path = "data-pipeline-aws"
        else:
            output_path = "data-pipeline-aws-beta"
        app_time_file = open("/home/hadoop/SHN-Data-Pipeline-1.0.271/output/%s/op-utils/app-time.conf" % output_path,
                             "w")
        for line in app_time_list:
            app_time_file.write(line + "\n")
        app_time_file.close()

    def scan_f_flag(self, site, jobs):
        app_time_list = []
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
            f_flag_minute = self.run_command(
                "cat %s/output/%s/op-utils/app-time.conf | grep '%s' | grep coordStart | head -1 " % (
                    self.build_folder, output_path, job))[-4:-1]
            if "System" in job:
                f_flag_day = datetime.now().strftime('%Y-%m-%d')
            else:
                f_flag_day = self.run_command("aws s3 ls %s/%s/ | tail -1 | awk '{print $4}' | cut -d'_' -f1" %
                                              (site_s3_path, mapping[job]))[-11:-1]
            if jobs[0] == "hourly":
                if "TxExport" in job:
                    f_flag_hour = self.run_command("aws s3 ls %s/%s/pdd=%s/ | tail -1 | awk '{print $4}'" %
                                                   (site_s3_path, mapping[job], f_flag_day))[5:6]
                else:
                    f_flag_hour = self.run_command("aws s3 ls %s/%s/d=%s/ | tail -1 | awk '{print $4}'" %
                                                   (site_s3_path, mapping[job], f_flag_day))[3:4]
            else:
                f_flag_hour = "00"
            app_start_time = datetime.strptime(f_flag_day + f_flag_hour, '%Y-%m-%d%H') + add_time
            app_end_time = app_start_time + timedelta(days=36524)
            app_time_list.append(
                "%s:    coordStart=%s:%s" % (job, app_start_time.strftime('%Y-%m-%dT%H'), f_flag_minute))
            app_time_list.append("%s:    coordEnd=%s:00Z" % (job, app_end_time.strftime('%Y-%m-%dT%H')))
        return app_time_list

    def deploy(self, site):
        pass
        # to import quick deploy tool

    @staticmethod
    def command_parser():
        usage = "\t%s [options]\nTool version:\t%s" % (sys.argv[0], "20180222")
        parser = OptionParser(usage)
        parser.add_option("-s", type="string", dest="site", help='Choose deploy target site')
        parser.add_option("-n", action="store_true", dest="new_deploy", help='Execute a new deploy on EMR')
        parser.add_option("-c", action="store_true", dest="change_build", help='Execute change build on EMR')
        if len(sys.argv) == 1:
            parser.print_help()
            print('\nQuick Start:')
            print('# To deploy on a new EMR as Production Site')
            print('python %s -s production -n' % os.path.basename(__file__))
            print('# To deploy on a new EMR as Beta Site')
            print('python %s -s beta -n' % os.path.basename(__file__))
            print('# To change build on Production Site')
            print('python %s -s production -c' % os.path.basename(__file__))
            print('# To change build on Production Site')
            print('python %s -s beta -c' % os.path.basename(__file__))
            exit()
        return parser.parse_args()[0]


if __name__ == "__main__":
    DT = DeployTool()
    main_job = DeployTool.command_parser()
    if not main_job.site or (main_job.site != "production" and main_job.site != "beta"):
        print('Please assign site as "production" or "beta.".')
    else:
        if main_job.new_deploy and not main_job.change_build:
            DT.get_build()
            DT.config_env(main_job.site)
            DT.set_app_time(main_job.site)
        elif not main_job.new_deploy and main_job.change_build:
            DT.get_build()
            DT.config_env(main_job.site)
            DT.set_app_time(main_job.site)
        else:
            print('Please choose one option for new deploy(-n)/change build(-c).')
