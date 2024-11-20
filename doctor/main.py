import logging
import os
from doctor import Doctor

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    Doctor().start()

if __name__ == '__main__':
    main()
