import subprocess


class DeployTool:
    AWS_PROD_S3_PATH = "s3://trs-production-us-west-2"
    AWS_BETA_S3_PATH = "s3://trs-production-beta-data-us-west-2"
    AWS_BUILD_PATH = "s3://eric-staging-us-west-2/build"
    AWS_SIGNATURE_PATH = "s3://eric-staging-us-west-2/signature"

    def run_command(self, cmd, show_command=True):
        if show_command:
            print(cmd)
        o = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = o.communicate()
        if result[1] != "":
            print(result)
        else:
            print(result[0])

    def get_build(self, build_number=""):
        command = "aws s3 cp " + self.AWS_BUILD_PATH + self.set_build(build_number) + cd "~/"
        self.run_command(command)

    def set_build(self, build_number):
        if not build_number:
            return self.run_command("aws s3 ls " + self.AWS_BUILD_PATH +
                                    " | grep 'SHN-Data-Pipeline' | sort | tail -1 | awk '{print $4}'")
        else:
            return "SHN-Data-Pipeline-" + build_number + ".tar.gz"

    def get_signature(self):
        pass

    def get_apptime(self, location):
        pass

    def set_apptime(self, start_time, day, week):
        pass

    def create_metadata(self, location):
        pass

    def msck_repair(self, location):
        pass

    def deploy_build(self, location):
        pass

    def rerun_failed_job(self):
        pass

    def clean_all_job(self):
        pass

    if __name__ == "__main__":
        print('this is main')
