import logging
import os
import subprocess

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    result = subprocess.run(['docker', 'ps'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))

if __name__ == '__main__':
    main()
