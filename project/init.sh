# TODO script to init a project

PACKAGE_NAME=tools

python3.11 -m venv .venv
python3.11 -m pip install poetry
poetry init --name=$PACKAGE_NAME --description='template' --author='benny <b1623546466@gmail.com>' --python='^3.11'
poetry add loguru pydantic
