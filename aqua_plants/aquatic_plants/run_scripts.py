import subprocess

# Replace 'script_to_run.py' with the actual name of your script
spiders = ['fishykart', 'bunnycart', 'finsnflora','aquabynature','aquaplant','aquaticplants','geturpet','sreepadma']
for spider in spiders:
    subprocess.run(['pipenv', 'run', 'scrapy', 'crawl', spider])