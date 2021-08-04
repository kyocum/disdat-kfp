echo "Building Disdat-KFP package locally"
rm -rf disdat_kfp.egg-info
rm -rf dist

python3 -m build


echo "upload to TestPyPi"
ss
echo "Create a new venv for testing"
rm -rf .testenv
python3 -m venv .testenv
source .testenv/bin/activate
pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple disdat-kfp

python3 -c "
from  disdat_kfp.caching_wrapper import Caching
print('Test Done')"

deactivate
echo "Clean up testing env"
rm -rf .testenv


if false;  then
  echo "upload to PyPi for real!"
fi