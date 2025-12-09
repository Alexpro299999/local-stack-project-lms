import os
import sys

def check_logs():
    venv_scripts = os.path.dirname(sys.executable)
    aws_cmd = os.path.join(venv_scripts, 'aws.cmd')
    
    print(f"Using AWS CLI at: {aws_cmd}")
    
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    cmd = f'"{aws_cmd}" --endpoint-url=http://localhost:4566 logs filter-log-events --log-group-name /aws/lambda/ProcessBikesData'
    
    print("\n--- LAMBDA LOGS ---")
    exit_code = os.system(cmd)
    
    if exit_code != 0:
        print("\nCould not fetch logs. Possible reasons:")
        print("1. Lambda has not started yet (wait a minute).")
        print("2. The log group does not exist (Lambda never ran).")

if __name__ == "__main__":
    check_logs()