from distutils.core import setup
setup(
  name = 'pyspark-easy',         # How you named your package folder (MyLib)
  packages = ['pyspark-easy'],   # Chose the same as "name"
  version = '0.8',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'Makes pyspark dataframe exploration easy',   # Give a short description about your library
  author = 'Nareshkumar Jayavelu',                   # Type in your name
  author_email = 'nareshkumarj90@gmail.com',      # Type in your E-Mail
  url = 'https://https://github.com/naresh-datanut/pyspark-easy',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/naresh-datanut/pyspark-easy/archive/0.8.tar.gz',    # I explain this later on
  keywords = ['pyspark', 'dataframe', 'summary','explore','EDA'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
          'pyspark',
          'texttable'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)
