import setuptools

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name='hissdb',
    version='0.0.1',
    description='a simple SQLite query builder with a few bells and whistles',
    author='Simon Raindrum Sherred',
    author_email='simonraindrum@gmail.com',
    license='None',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/raindrum/hissdb",
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database :: Front-Ends',
    ],
)
