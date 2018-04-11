class DeployTool:
    def get_build(self):
        pass

    def set_build(self):
        pass

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
