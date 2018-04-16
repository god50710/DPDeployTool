import os
import subprocess
import sys
from optparse import OptionParser


class DeployTool(object):
    AWS_PROD_S3_PATH = "s3://trs-production-us-west-2/"
    AWS_BETA_S3_PATH = "s3://trs-production-beta-data-us-west-2/"
    AWS_BUILD_PATH = "s3://eric-staging-us-west-2/build/"
    AWS_SIGNATURE_PATH = "s3://eric-staging-us-west-2/signature/"
    PROD_ENV_PATH = "output/data-pipeline-aws/op-utils/env"
    BETA_ENV_PATH = "output/data-pipeline-aws-beta/op-utils/env"
    BETA_OOZIE_PATH = "output/data-pipeline-aws-beta/oozie/*/job.properties"
    BETA_SCRIPT_PATH = "output/data-pipeline-aws-beta/script/hql_external_partition.sh"
    BETA_HQL_PATH = "output/data-pipeline-aws-beta/hql/*.hql"
    VERSION = "20180414"

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
            raise Exception
        else:
            return result[0]

    def get_build(self):
        build_file = self.run_command("aws s3 ls %s | grep 'SHN-Data-Pipeline' | sort | tail -1 | awk '{print $4}'"
                                      % self.AWS_BUILD_PATH)[:-1]
        if not build_file:
            raise Exception('No available build to deploy')

        self.run_command("aws s3 cp %s%s ~" % (self.AWS_BUILD_PATH, build_file))
        self.run_command("tar -zxvf ~/%s" % build_file)
        print("[Info] Build %s is ready" % build_file.split('.tar')[0])
        self.build_folder = "~/%s" % build_file.split('.tar')[0]
        self.build_version = self.build_folder.split('1.0.')[1]

    def config_env(self, site):
        if site == "production":
            self.run_command("cp %s/%s/aws-production.sh %s/%s/$(whoami)\@$(hostname).sh"
                             % (self.build_folder, self.PROD_ENV_PATH, self.build_folder, self.PROD_ENV_PATH))
            self.run_command("echo 'OOZIE_APP_EXT=.AWSProduction%s' >> %s/%s/$(whoami)\@$(hostname).sh" %
                             (self.build_version,self.build_folder, self.PROD_ENV_PATH))
        else:
            self.run_command("cp %s/%s/aws-production-beta-data.sh %s/%s/$(whoami)\@$(hostname).sh"
                             % (self.build_folder, self.BETA_ENV_PATH, self.build_folder, self.BETA_ENV_PATH))
            self.run_command("echo 'OOZIE_APP_EXT=.AWSBeta%s' >> %s/%s/$(whoami)\@$(hostname).sh" %
                             (self.build_version,self.build_folder, self.BETA_ENV_PATH))
            self.run_command("sed -i 's/^cntLowerbound=.*$/cntLowerbound=0/g' %s/%s" %
                             (self.build_folder, self.BETA_OOZIE_PATH))
            self.run_command("sed -i 's/ --driver-memory 12G --executor-memory 12G//g' %s/%s" %
                             (self.build_folder, self.BETA_SCRIPT_PATH))
            self.run_command("sed -i '/SET hive.tez.java.opts=-Xmx10240m;/d' %s/%s" %
                             (self.build_folder, self.BETA_HQL_PATH))

    def config_app_time(self, site):
        pass
        # get all table app time by f_ flags

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
        elif not main_job.new_deploy and main_job.change_build:
            DT.get_build()
            DT.config_env(main_job.site)
        else:
            print('Please choose one option for new deploy(-n)/change build(-c).')
