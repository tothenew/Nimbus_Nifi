from setuptools import setup, find_packages
setup(name="nifi-ingestion-frmk",
      version="0.0",
      description="testing",
      long_description="testing it for practice purpose",
      author="gaurav",
      packages=find_packages(exclude=["nifi-ingestion-frmk"]),
      include_package_data=True,
      install_requires=["nipyapi"],
      entry_points={'console_scripts': ['nimbus_env=nifi.setup_nifi_environment:main',
                                        "run_ingestion=nifi.run_ingestion:main"]})
