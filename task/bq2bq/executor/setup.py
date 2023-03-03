from setuptools import setup, find_packages


with open("./bumblebee/version.py") as fp:
    exec(fp.read())


requirements = []
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

if __name__ == "__main__":
    setup(
        name="bumblebee",
        version=VERSION,
        author="goto",
        author_email="gotocompany@gmail.com",
        description="BigQuery to BigQuery Transformation client",
        packages=find_packages(),
        install_requires=requirements,
        test_suite='nose.collector',
        tests_require=['nose','coverage']
    )