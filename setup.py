from setuptools import setup, find_packages

setup(
    name='nntp-indexer',
    version='1.0.0',
    description='Library for indexing and searching Usenet (NNTP) article headers',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/yourusername/nntp-indexer',
    packages=find_packages(),
    install_requires=[
        'orjson>=3.9.0',
    ],
    python_requires='>=3.10',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
