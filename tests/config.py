# base image for caching containers
BASE_IMAGE = 'docker.intuit.com/docker-rmt/python:3.8'
# the local artifact folder root for both integration and unit testing
ARTIFACT_DIR = './test_artifacts'

# for integration testing only, must be a bucket that the EKS cluster can access
S3_BUCKET = 'mint-ai-disdatnoop-e2e-435945521637'
# For unit testing, must be a bucket that you can access with local credentials
UNIT_TEST_S3_BUCKET = 's3://disdat-kubeflow-playground'
