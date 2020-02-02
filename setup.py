import setuptools


__packagename__ = 'water-healer'

setuptools.setup(
    name = __packagename__,
    packages = setuptools.find_packages(),
    version = '0.0.1',
    python_requires = '>=3.7.*',
    description = 'Extension of Kafka Streamz to update consumer offset for successful sink',
    author = 'huseinzol05',
    author_email = 'husein.zol05@gmail.com',
    url = 'https://github.com/huseinzol05/water-healer',
    install_requires = ['streamz', 'tornado'],
    license = 'MIT',
    classifiers = [
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)