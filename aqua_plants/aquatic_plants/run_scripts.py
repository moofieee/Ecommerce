import subprocess
from prefect import flow, task

# Replace 'script_to_run.py' with the actual name of your script
spiders = ['fishykart', 'bunnycart', 'finsnflora','aquabynature','aquaplant','aquaticplants','geturpet','sreepadma']
for spider in spiders:
    subprocess.run(['pipenv', 'run', 'scrapy', 'crawl', spider])
 


# @flow()
# def run_scripts():
#     spiders = ['fishykart', 'bunnycart', 'finsnflora', 'aquabynature', 'aquaplant', 'aquaticplants', 'geturpet','sreepadma']
#     for spider in spiders:
#         subprocess.run(['pipenv', 'run', 'scrapy', 'crawl', spider])


# if __name__ == "__main__":
#     run_scripts.serve(name="run_scripts", cron="0 9 * * *")