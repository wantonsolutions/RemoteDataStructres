from setuptools import setup, find_packages

setup(
    name='remotedatastructures',
    version='0.1.0',
    description='Python interface to remote data structures',
    author='Wantonsolutions (Stewart Grant)',
    author_email='stewbertgrant@gmail.com',
    packages=find_packages("./"),
    install_requires=['numpy', 'matplotlib', 'gitpython'],
)