import time
import subprocess
import logging

logging.basicConfig(
    filename="scheduler.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

ETL_COMMAND = ["python3", "/home/nineleaps/Desktop/case_study_2/src/etl.py"]

def run_scheduler():
    logging.info("Scheduler started")

    while True:
        logging.info("Triggering ETL job")
        print("Running ETL job...")

        subprocess.run(ETL_COMMAND)

        logging.info("ETL job finished. Sleeping for 5 hours")
        print("Sleeping for 5 hours...\n")

        time.sleep(5 * 60 * 6)  # 5 hours

if __name__ == "__main__":
    run_scheduler()