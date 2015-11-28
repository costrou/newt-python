from distutils.core import setup
import os

long_description = """ newt-python is a thin pythonic wrapper arround the NEWT NERSC api """

packages = []
for dirname, diranmes, filenames in os.walk('newt'):
    if '__init__.py' in filenames:
        packages.append(dirname.replace('/', '.'))

package_dir = {'newt': 'newt'}

setup(name='newt',
      version="0.0.1",
      description='NEWT NERSC API',
      url='https://github.com/costrou/newt-python',
      maintainer='Christopher Ostrouchov',
      maintainer_email='chris.ostrouchov+newt@gmail.com',
      license='LGPLv2.1+',
      platforms=['linux'],
      packages=packages,
      package_dir=package_dir,
      long_description=long_description)
