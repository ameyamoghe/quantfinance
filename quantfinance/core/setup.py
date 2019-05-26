'''
Created on Jan 24, 2018

@author : amoghe

'''

from distutils.core import setup
from Cython.Build import cythonize

setup(
    ext_modules = cythonize("cqueue.pxd")
)