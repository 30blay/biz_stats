pip install virtualenv
virtualenv .env
source .env/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$PYTHONPATH:$(pwd)