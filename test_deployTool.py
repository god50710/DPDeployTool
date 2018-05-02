from unittest import TestCase
from unittest.mock import MagicMock

from deploy_tool import DeployTool


class TestDeployTool(TestCase):
    def test_disable_stunnel_pid_exist(self):
        DT = DeployTool
        DT.run_command = MagicMock()
        DT.run_command.return_value = 5308
        DeployTool.disable_stunnel()
        DT.run_command.assert_called_with("ps -ef | grep [s]tunnel | awk '{print $2}' | xargs sudo kill -9",
                                          throw_error=False)
        if not DT.run_command.call_count == 2:
            raise Exception

    def test_disable_stunnel_pid_not_exist(self):
        DT = DeployTool
        DT.run_command = MagicMock()
        DT.run_command.return_value = None
        DeployTool.disable_stunnel()
        DT.run_command.assert_called_with("ps -ef | grep [s]tunnel | awk '{print $2}'")
        if not DT.run_command.call_count == 1:
            raise Exception

    def test_repair_partitions_with_limits_database(self):
        self.assertRaises(Exception, DeployTool.repair_partition, "beta", "dp")
        self.assertRaises(Exception, DeployTool.repair_partition, "production", "dp_beta")

    def test_repair_partitions_with_allow_database(self):
        DT = DeployTool
        DT.run_command = MagicMock()
        DT.repair_partition("beta", "dp_beta")
        DT.run_command.assert_called_with('beeline -u "jdbc:hive2://localhost:10000/" '
                                          '--silent=true -e "msck repair table dp_beta.t_traffic_stats_daily;"',
                                          throw_error=False)

    def test_repair_partitions_with_all_database(self):
        DT = DeployTool
        DT.run_command = MagicMock()
        DT.repair_partition("dp", "all")
        DT.run_command.assert_called_with('beeline -u "jdbc:hive2://localhost:10000/" '
                                          '--silent=true -e "msck repair table datalake.iotlog;"',
                                          throw_error=False)

    def test_add_cronjob_add_signature(self):
        DT = DeployTool
        DT.run_command = MagicMock()
        DT.run_command.return_value = ""
        DT.add_cronjob("beta", "/home/hadoop/SHN-Data-Pipeline-1.0.530")
        if "/home/hadoop/update_signature/bg_executor.sh beta" not in str(DT.run_command.call_args_list):
            raise Exception

    def test_add_cronjob_modify_geoip(self):
        DT = DeployTool
        DT.run_command = MagicMock()
        DT.run_command.return_value = "exist"
        DT.add_cronjob("production", "/home/hadoop/SHN-Data-Pipeline-1.0.530")
        if "/trs/update_geoip/geoip_bg_executor_with_mail.sh production" not in str(DT.run_command.call_args_list):
            raise Exception