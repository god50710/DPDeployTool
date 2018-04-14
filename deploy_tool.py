import os
import subprocess
import sys
from optparse import OptionParser

AWS_PROD_S3_PATH = "s3://trs-production-us-west-2/"
AWS_BETA_S3_PATH = "s3://trs-production-beta-data-us-west-2/"
AWS_BUILD_PATH = "s3://eric-staging-us-west-2/build/"
AWS_SIGNATURE_PATH = "s3://eric-staging-us-west-2/signature/"
VERSION = "20180414"


def run_command(cmd, show_command=True):
    if show_command:
        print(cmd)
    o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = o.communicate()
    if result[1] != "":
        print(result)
    else:
        return result[0]


def get_build(build_number=""):
    command = "aws s3 cp %s%s ~" % (AWS_BUILD_PATH, set_build(build_number)[:-1])
    run_command(command)


def set_build(build_number=""):
    if not build_number:
        return run_command("aws s3 ls %s | grep 'SHN-Data-Pipeline' | sort | tail -1 | awk '{print $4}'"
                           % AWS_BUILD_PATH)
    else:
        return "SHN-Data-Pipeline-%s.tar.gz" % build_number


def config_env(site):
    pass


def config_app_time(site):
    pass


def deploy(site):
    pass


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
    main_job = command_parser()
    if not main_job.site or (main_job.site != "production" and main_job.site != "beta"):
        print('Please assign site as "production" or "beta.".')
    else:
        if main_job.new_deploy and not main_job.change_build:
            get_build()
        elif not main_job.new_deploy and main_job.change_build:
            get_build()
        else:
            print('Please choose one option for new deploy(-n)/change build(-c).')
