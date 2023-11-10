pipenv run flake8 . 
pipenv run isort --profile black .
pipenv run pyright
pipenv run pytest