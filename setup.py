from setuptools import setup, find_packages

install_requires = [
    'PynamoDB>=3.2.1'
]

setup(
    name='InPynamoDB',
    version=__import__('inpynamodb').__version__,
    python_requires=">=3.5",
    packages=find_packages(),
    author='Sunghyun Lee',
    author_email='jolacaleb@gmail.com',
    url="https://github.com/MyMusicTaste/InPynamoDB",
    description='Asynchronous implementation of PynamoDB',
    long_description=open('README.md').read(),
    zip_safe=False,
    license='MIT',
    keywords='python dynamodb amazon async',
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 1 - Developing',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: MIT License',
    ]
)
