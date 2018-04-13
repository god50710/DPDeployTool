from unittest import TestCase
from deploy_tool import DeployTool


class TestDeployTool(TestCase):
    def test_get_build_with_build_number(self):
        build_number = "1.0.270"
        TestCase.assertEqual(self, "SHN-Data-Pipeline-1.0.270.tar.gz", DeployTool.get_build(build_number))
