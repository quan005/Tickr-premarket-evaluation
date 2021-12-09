from setuptools import setup
from setuptools import find_namespace_packages
import sys

# load the README file.
with open(file="README.md", mode="r") as fh:
    long_description = fh.read()

if sys.platform == "win32":
    platform_supported = True
    lib_talib_name = 'ta_libc_cdr'
    include_dirs = [r"c:\ta-lib\c\include"]
    lib_talib_dirs = [r"c:\ta-lib\c\lib"]

setup(

    name='ticker-trading-robot',

    author='DaQuan Freeman',

    author_email='daquan.dev@gmail.com',

    version='0.1.0',

    description='A trading robot built for Python that uses the TD Ameritrade API.',

    url='https://github.com/quan005/Tickr-premarket-evaluation.git',

    long_description=long_description,

    long_description_content_type="text/markdown",

    install_requires=[
        'pandas==1.3.4',
        'yfinance==0.1.67',
        'nltk==3.6.5',
        'beautifulsoup4==4.10.0',
        'td-ameritrade-python-api==0.3.5',
        'numpy==1.21.4',
        'confluent-kafka==1.7.0'
    ],

    keywords='finance, td ameritrade, api, trading robot',

    packages=find_namespace_packages(
        include=['trader_components', 'tests'],
        exclude=['configs*']
    ),

    include_package_data=True,

    python_requires='>=3.8',

    classifiers=[

        # I can say what phase of development my library is in.
        'Development Status :: 3 - Alpha',

        # Here I'll add the audience this library is intended for.
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Financial and Insurance Industry',

        # Here I'll define the license that guides my library.
        'License :: OSI Approved :: MIT License',

        # Here I'll note that package was written in English.
        'Natural Language :: English',

        # Here I'll note that any operating system can use it.
        'Operating System :: OS Independent',

        # Here I'll specify the version of Python it uses.
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',

        # Here are the topics that my library covers.
        'Topic :: Database',
        'Topic :: Education',
        'Topic :: Office/Business'

    ]
)
