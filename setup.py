from setuptools import setup

setup(
    name="wsmonitor",
    version="0.0.1",
    author="guilaume pernot",
    author_email="gpernot@praksys.org",
    description="Website monitor.",
    license="GPLv3+",
    packages=['wsmonitor'],
    setup_requires=['psycopg2', 'click'],
    tests_require=['pytest'],
    install_requires=['psycopg2', 'click'],
    package_data={'wsmonitor': ['lib/wsmonitor-checker.service',
                                'lib/wsmonitor-dbupdate.service']},
    classifiers=[
        "Development Status :: 1 - Planning",
        "Topic :: System :: Networking :: Monitoring",
        "Programming Language :: Python",
        "License :: OSI Approved :: GNU General Public License v3 or "
        "later (GPLv3+)",
    ],
    entry_points={
        'console_scripts': [
            'wsmonitor=wsmonitor.cli:cli',
            'wsmonitor-checker-daemon=wsmonitor.checker:checker',
            'wsmonitor-dbupdate-daemon=wsmonitor.dbupdate:dbupdate',
        ],
    },

)
