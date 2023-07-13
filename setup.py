from setuptools import setup

setup(
    name='diesel_wscr',
    version='0.1',
    packages=[''],
    install_requires=[
        'requests',
        'beautifulsoup4',
        'pandas',
        'schedule'
    ],
    entry_points={
        'console_scripts': [
            'diesel_wscr = core_code.py:main',
        ],
    },
)
