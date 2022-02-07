from setuptools import setup, find_packages

setup(name='tl21e',
      version='0.1',
      python_requires='>=3.10',
      install_requires=[
          'pyaml', 'requests',
          'rich', 'thefuzz', 'pypinyin',
          'tencentcloud-sdk-python-common',
          'tencentcloud-sdk-python-asr',
          'python-levenshtein', 'tatsu', 'jinja2'],
      tests_require=[
          'pytest',
          'pytest-asyncio'],
      entry_points=dict(console_scripts=['tl21e = tl21e.cli:exec']),
      packages=find_packages())
