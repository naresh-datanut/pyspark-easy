from distutils.core import setup

setup(
    name='pyspark_easy',  # How you named your package folder (MyLib)
    packages=['pyspark_easy'],  # Chose the same as "name"
    version='1.5',  # Start with a small number and increase it with every change you make
    license='MIT',  # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    description='Makes pyspark dataframe exploration easy',  # Give a short description about your library
    author='Nareshkumar Jayavelu',  # Type in your name
    author_email='nareshkumarj90@gmail.com',  # Type in your E-Mail
    url='https://github.com/naresh-datanut/pyspark-easy',  # Provide either the link to your github or to your website
    download_url='https://github.com/naresh-datanut/pyspark-easy/archive/1.5.tar.gz',  # I explain this later on
    keywords=['pyspark', 'dataframe', 'evaluation', 'model', 'classification', 'multiclass classification',
              'binary classification', 'results', 'search','pyspark utility','util','pyspark date','quick', 'summary', 'explore', 'EDA'],
    # Keywords that define your package best
    install_requires=['pyspark',
                      'texttable',
                      'DateTime',
                      'python-dateutil',
                      'numpy',
                      'seaborn',
                      'matplotlib',
                      'pandas',
                      'scikit-plot'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Intended Audience :: Developers',  # Define that your audience are developers
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',  # Again, pick a license
        'Programming Language :: Python :: 3',  # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
