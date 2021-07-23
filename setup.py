from setuptools import setup, find_packages

setup(name='olpy',
      version='0.0.1',
      description='Python tools for OpenLattice data scientists',
      author='OpenLattice',
      author_email='info@openlattice.com',
      packages=find_packages(),
      dependency_links=[
        'https://github.com/Lattice-Works/butter-fingers'
      ],
      package_data={
        'olpy': ['flight/resources/aesthetics.yaml', 'simulate/media/nicknames.csv', 'pipelines/pipeline_config.yaml'],
      },
      install_requires=[
          'auth0-python',
          'cryptography',
          'setuptools_rust'
      ],
      setup_requires=[
          'cryptography',
          'setuptools_rust'
      ],
      zip_safe=False
)
