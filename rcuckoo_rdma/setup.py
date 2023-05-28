from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy

import os
xx_hash_dir = './xxHash'
xx_hash_abs_dir = os.path.abspath(xx_hash_dir)

extensions = [
    Extension('chash', ['hash_wrapper.pyx', 'hash.cpp'],
              include_dirs=[xx_hash_abs_dir],
              libraries=['xxhash'],
              extra_compile_args=['-std=c++11'],
              language='c++'
              ),
    Extension('ctables', ['tables_wrapper.pyx', 'tables.cpp'],
              extra_compile_args=['-std=c++11'],
              language='c++'
              ),
    # Extension('csearch', ['search_wrapper.pyx', 'search.cpp'],
    #           extra_compile_args=['-std=c++11'],
    #           language='c++'
    #           ),
]



setup(
    name='ctables',
    ext_modules=cythonize(extensions, language_level=3),
    # extra_compile_args=["-w", '-g'],
)

# setup(
#     name='csearch',
#     ext_modules=cythonize(extensions, language_level=3),
#     # extra_compile_args=["-w", '-g'],
# )