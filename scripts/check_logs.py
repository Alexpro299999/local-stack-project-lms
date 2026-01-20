import os
import sys
import logging
import subprocess
from script_config import (
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_ENDPOINT
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_logs():
    """
    fetches and displays lambda logs using aws cli via subprocess.
    """
    venv_scripts = os.path.dirname(sys.executable)
    aws_cmd = os.path.join(venv_scripts, 'aws')

    if not os.path.exists(aws_cmd):
        aws_cmd = 'aws'

    logger.info(f"using aws cli: {aws_cmd}")

    env = os.environ.copy()
    env['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY
    env['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_KEY
    env['AWS_DEFAULT_REGION'] = AWS_REGION

    cmd = [
        aws_cmd,
        f'--endpoint-url={S3_ENDPOINT}',
        'logs',
        'filter-log-events',
        '--log-group-name',
        '/aws/lambda/ProcessBikesData'
    ]

    logger.info("fetching lambda logs...")

    try:
        result = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error("could not fetch logs.")
        logger.debug(f"stderr: {e.stderr}")
    except FileNotFoundError:
        logger.error("aws cli not found. please install awscli or awscli-local.")


if __name__ == "__main__":
    check_logs()