import os.path
from setuptools import setup, find_packages

project_root = os.path.abspath(os.path.dirname(__file__))

install_requires = []
with open(os.path.join(project_root, 'requirements.txt'), 'r') as f:
    for line in f:
        install_requires.append(line.rstrip())


with open(os.path.join(project_root, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name="pystalk",
    version="0.7.0",
    author="EasyPost",
    author_email="oss@easypost.com",
    url="https://github.com/easypost/pystalk",
    description="Very simple beanstalkd client",
    long_description=long_description,
    long_description_content_type='text/markdown',
    license="ISC",
    install_requires=install_requires,
    packages=find_packages(exclude=['tests', 'tests.*']),
    python_requires='>=3.6, <4',
    project_urls={
        'CI': 'https://travis-ci.com/EasyPost/pystalk',
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Intended Audience :: System Administrators",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "License :: OSI Approved :: ISC License (ISCL)",
    ]
)
