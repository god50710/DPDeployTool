import json
import re
import sys
import time
import ConfigParser
import subprocess
from optparse import OptionParser
from datetime import datetime
from datetime import timedelta

adhoc_command = " \
\n# To check one table \
\naws --profile trs-admin athena start-query-execution \
--query-string \"show partitions trs_src.router_security_to_trs\" \
--result-configuration OutputLocation=s3://aws-athena-query-results-502803443756-us-west-2/addPartition/ \
\n \
\n# To repair all table in DB \
\nmsck repair table ${dbName}.${t}; \
\n \
\n# To repair one table \
\nALTER TABLE datalake.akamai_rgom ADD IF NOT EXISTS PARTITION (dt='2018-01-03-05') \
\n \
\n# How to manual check: \
\naws --profile trs-ops-production s3 ls s3://trs-production-us-west-2/Application/shnprj_spn/hive/pm_src.db/t_dpi_config_stats_by_country_weekly/ \
\n \
\n# How to check logs from CloudWatch(Download from https://github.com/jorgebastida/awslogs or 'yum install awslogs'): \
\nawslogs get /aws/lambda/staging-partitions-ready --aws-region us-west-2 --profile trs-ops-stage  --filter-pattern='sns' -s '1h'|grep 'EventSubscriptionArn\|table\|partitions\|database\|Timestamp'|sed 's/{\"Records\".*\", \"Timestamp\"//g'|sed 's/\"Signature\".*\"Message\"//g'|sed 's/\"MessageAttributes\".*//g'|sed 's/\\\\\"//g'|sed 's/\/aws\/lambda\///g' \
\n"

PARTITION_DT_FORMAT = [
    'pdd=%Y-%m-%d/phh=%H',
    'd=%Y-%m-%d/h=%H',
    'dt=%Y-%m-%d-%H',
    'pdd=%Y-%m-%d',
    'dt=%Y-%m-%d',
    'd=%Y-%m-%d',
]

PRODUCTION_DATABES = ["datalake", "dp", "dp_ex", "trs_src"]
STAGING_BETA_DATABES = ["datalake", "dp", "dp_beta", "trs_src", "pm_src"]

PARTITION_STEP = ''


def run_command(cmd, SHOW_OUTPUT=False, DEBUG=False):
    global ops_DEBUG
    DEBUG = ops_DEBUG
    if SHOW_OUTPUT or DEBUG: print(cmd)
    o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = o.communicate()
    if DEBUG:
        if result[1] != "":
            print(result)
        else:
            print(result[0])
    return result[0]


def repair_partition(db_table):
    cmd = 'beeline -u "jdbc:hive2://localhost:10000/" -e "msck repair table %s;"' % (db_table)
    print(cmd)


def get_result_by_aws_id(PROFILE, ID):
    time.sleep(3)
    cmd = "aws --profile %s athena get-query-results --query-execution-id %s" % (PROFILE, ID)
    return run_command(cmd)


def run_aws_job(cmd):
    query_result = run_command(cmd)
    if re.search('QueryExecutionId', query_result):
        job_id = re.findall('"QueryExecutionId": "(.*)"', query_result)[-1]
    else:
        job_id = query_result
    return job_id


def test_sample():
    content = ''
    with open("sample.log", 'r') as f:
        for each in f:
            content += each
    return content


def retrive_partition_info(aws_result):
    ''' collect exist partitions into list '''
    exist_partitions = []
    if aws_result.has_key('ResultSet'):
        d_ResultSet = aws_result['ResultSet']
        if d_ResultSet.has_key('Rows'):
            for row in aws_result['ResultSet']['Rows']:
                for d in row['Data']:
                    dtstr = d['VarCharValue']
                    for f in PARTITION_DT_FORMAT:
                        try:
                            t = datetime.strptime(dtstr, f)
                            break
                        except BaseException:
                            t = None
                            continue
                    if t is None:
                        raise Exception("dt format error")
                    part = t.strftime('%Y-%m-%d-%H')
                    exist_partitions.append({part: t})  # ex. {'2017-11-18-08': datetime.datetime(2017, 11, 18, 8, 0)}
        else:
            print(aws_result)
            return False
    else:
        print(aws_result)
        return False
    return exist_partitions


def retrive_flag_info(aws_result):
    exist_partitions = []
    patterns = ["\/d=(\d\d\d\d-\d\d-\d\d)\/_SUCCESS", "\/d=(\d\d\d\d-\d\d-\d\d)\/h=(\d\d)\/_SUCCESS"]
    # /d=2018-03-05/h=22/_SUCCESS
    # /d=2018-03-05/_SUCCESS
    aws_result_list = aws_result.split('\n')
    for each in (aws_result_list):
        for index, ptn in enumerate(patterns):
            ptn = re.compile(ptn)
            if re.search(ptn, each):
                if index == 0:
                    date_d = re.findall(ptn, each)[-1]
                    t = '%s-00' % date_d
                else:
                    (date_d, date_h) = re.findall(ptn, each)[-1]
                    t = '%s-%s' % (date_d, date_h)
                t = datetime.strptime(t, '%Y-%m-%d-%H')
                part = t.strftime('%Y-%m-%d-%H')
                exist_partitions.append({part: t})
            else:
                continue
    return exist_partitions


def generate_report(aws_result, check_option, PARTITION_STEP, start_dt):
    ''' [check_option] True: check partition, False: check SNS flag'''
    if check_option:
        partitions = retrive_partition_info(aws_result)
    else:
        partitions = retrive_flag_info(aws_result)

    if partitions:
        partitions = sorted(partitions)
    else:
        print("No table found!")
        return 0

    if not start_dt:
        start_dt = partitions[0].values()[0]
        n = start_dt
    else:
        n = datetime.strptime(start_dt, '%Y-%m-%d')

    print('    from {}'.format(n), 'to {}'.format(partitions[-1].keys()[0]))
    end_dt = partitions[-1].values()[0]

    part_dt = {}
    for item in partitions:
        part_dt[item.keys()[0]] = item.values()[0]

    ''' match any date lost one by one '''
    lost_list = []
    while n.date() < end_dt.date():
        n_str = n.strftime('%Y-%m-%d-%H')
        if n_str not in part_dt:  # and part != partitions[-1].keys()[0]:
            lost_list.append('{}'.format(n_str))
        n = get_next(n, PARTITION_STEP)
    if lost_list:
        print('Lost list:', lost_list)
        return True
    return False


def get_next(dt, step):
    if step == 'hour':
        return dt + timedelta(hours=1)
    if step == 'day':
        return dt + timedelta(days=1)
    if step == 'week':
        return dt + timedelta(days=7)
    raise Exception('step value({}) error'.format(step))


def job_dispatcher(profile, account_id, db_table, check_partition, check_flag):
    (db, table) = re.split('\.', db_table)
    job_frequency = config.get(db, table)
    if check_partition:
        command = 'aws --profile %s athena start-query-execution --query-string "show partitions %s" \
--result-configuration OutputLocation=s3://aws-athena-query-results-%s-%s/addPartition/' \
                  % (profile, db_table, account_id, "us-west-2")
        result = get_result_by_aws_id(profile, run_aws_job(command))
        try:
            result = json.loads(result)
        except:
            print("json.load exception!")
            print(result)
            return 0
    elif check_flag:
        command = 'aws --profile %s s3 ls s3://trs-%s-%s/Application/shnprj_spn/hive/%s.db/%s/ \
--recursive | grep _SUCCESS' % (profile, "production", "us-west-2", db, table)
        result = run_aws_job(command)
    else:
        print('Please at least assign -c(check partitions) or -f(check SNS flag)')
    return result, job_frequency


def command_parser():
    '''command parser for common usage'''
    usage = "\t%s [options]\nTool version:\t%s" % (sys.argv[0], "20180227")
    parser = OptionParser(usage)
    parser.add_option("-d", action="store_true", dest="debug", help='debug mode, print more information')
    parser.add_option("-r", action="store_true", dest="repair", help='repair a partition')
    parser.add_option("-t", action="store", type="string", dest="table",
                      help='choose a specific table for repair/check, ex. trs_src.router_security_to_trs')
    parser.add_option("-c", action="store_true", dest="check_partition", help='check partition lost')
    parser.add_option("-s", action="store", type="string", dest="start",
                      help='choose a start time (suggest to use Sunday for checking all tables) for check partitions (ex. 2018-03-04)')
    parser.add_option("-a", action="store", type="string", dest="account",
                      help='choose an AWS account: Production, Stage, or Beta')
    parser.add_option("-p", action="store", type="string", dest="profile",
                      help='choose an AWS configurate profile: trs-ops-production, trs-ops-stage, or trs-ops-beta')
    parser.add_option("-f", action="store_true", dest="check_flag", help='check if _SUCCESS flag are exist')

    if len(sys.argv) == 1:
        parser.print_help()
        print(adhoc_command)
        exit()
    return parser.parse_args()[0]


if __name__ == '__main__':
    OPS = command_parser()

    ''' Read information from configured file '''
    config = ConfigParser.ConfigParser()
    config.read('info.ini')
    total_section = config.sections()

    if OPS.debug:
        global ops_DEBUG
        ops_DEBUG = OPS.debug
    else:
        ops_DEBUG = False

    if OPS.repair:
        if OPS.table:
            repair_partition(OPS.table)
        else:
            print('Please choose which table (ex. -t trs_src.router_security_to_trs)')
    else:
        if OPS.profile and OPS.account:
            ops_account_id = config.get("ROLE", OPS.account)
            if OPS.table:
                (result, frequency) = job_dispatcher(OPS.profile, ops_account_id, OPS.table, OPS.check_partition,
                                                     OPS.check_flag)
                if_need_to_repair = generate_report(result, OPS.check_partition, frequency, OPS.start)
                if if_need_to_repair:
                    repair_partition(OPS.table)
                    print()
            else:
                print('Check all tables......')
                if OPS.account == 'Production':
                    DATABES = PRODUCTION_DATABES
                else:
                    DATABES = STAGING_BETA_DATABES
                LENGTH_DATABES = len(DATABES)
                for DB_index, DB in enumerate(DATABES, 1):
                    LENGTH_DB = len(config.items(DB))
                    for item_index, item in enumerate(config.items(DB), 1):
                        table = item[0]
                        frequency = item[1]
                        db_table = "%s.%s" % (DB, table)
                        print('### [%s] DB(%s/%s)-TABLE(%s/%s): %s ###' % (
                        OPS.account, DB_index, LENGTH_DATABES, item_index, LENGTH_DB, db_table))
                        (result, frequency) = job_dispatcher(OPS.profile, ops_account_id, db_table, OPS.check_partition,
                                                             OPS.check_flag)
                        if_need_to_repair = generate_report(result, OPS.check_partition, frequency, OPS.start)
                        if if_need_to_repair:
                            repair_partition(db_table)
                            print()
        else:
            print(
                'Please assign profile(ex.-p trs-ops-production), target table(ex. -t dp.t_device_hourly) and AWS account(ex. -a Production) ')
