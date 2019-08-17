from distutils.core import setup
setup(
	name='sparkaid',
	packages=['starfox.sparkaid'],
	version='0.1',
	license='gpl-3.0',
	description='utils for working with Spark',
	author='Averell Levan',
	author_email='lvhuyen@gmail.com',
	url='https://github.com/lvhuyen/sparkaid',
	download_url='https://github.com/lvhuyen/SparkAid/archive/v0.1-alpha.tar.gz',
	keywords=['SPARK', 'DATAFRAME', 'MANIPULATE'],
	install_requires=[
		'pyspark'
	],
	classifiers=[
		'Development Status :: 3 - Alpha',
		'Intended Audience :: Developers',
		'Topic :: Software Development :: Build Tools',
		'License :: GNU GPL 3 :: MIT License',
		'Programming Language :: Python :: 3',
		'Programming Language :: Python :: 3.4',
		'Programming Language :: Python :: 3.5',
		'Programming Language :: Python :: 3.6',
	],
)