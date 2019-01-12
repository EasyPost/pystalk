from setuptools import setup, find_packages


install_requires = []
with open('requirements.txt', 'r') as f:
    for line in f:
        install_requires.append(line.rstrip())


setup(
    name="pystalk",
    version="0.5.1",
    author="EasyPost",
    author_email="oss@easypost.com",
    url="https://github.com/easypost/pystalk",
    description="Very simple beanstalkd client",
    license="ISC",
    install_requires=install_requires,
    packages=find_packages(exclude=['tests']),
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, <4',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Intended Audience :: System Administrators",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "License :: OSI Approved :: ISC License (ISCL)",
    ]
)
