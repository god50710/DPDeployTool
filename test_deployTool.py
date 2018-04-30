from unittest import TestCase, mock

from deploy_tool import DeployTool


class TestDeployTool(TestCase):
    @mock.patch('deploy_tool.DeployTool.run_command')
    def test_disable_stunnel_pid_exist(self, mock_run_command):
        mock_run_command.return_value = 5308
        DeployTool.disable_stunnel()
        mock_run_command.assert_called_with("ps -ef | grep [s]tunnel | awk '{print $2}' | xargs sudo kill -9",
                                            throw_error=False)

    @mock.patch('deploy_tool.DeployTool.run_command')
    def test_disable_stunnel_pid_not_exist(self, mock_run_command):
        mock_run_command.return_value = None
        DeployTool.disable_stunnel()
        mock_run_command.assert_called_with("ps -ef | grep [s]tunnel | awk '{print $2}'")

    def test_repair_partitions_with_limits_database(self):
        self.assertRaises(Exception, DeployTool.repair_partition, "beta", "dp")

    @mock.patch('deploy_tool.DeployTool.run_command')
    def test_repair_partitions_with_allow_database(self, mock_run_command):
        DeployTool.repair_partition("beta", "dp_beta")
        mock_run_command.assert_called_with('beeline -u "jdbc:hive2://localhost:10000/" '
                                            '--silent=true -e "msck repair table dp_beta.t_traffic_stats_daily;"',
                                            throw_error=False)
