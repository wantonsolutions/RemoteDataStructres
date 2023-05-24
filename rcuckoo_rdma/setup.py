from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy


extensions = [
    Extension('chash', ['hash_wrapper.pyx', 'hash.cpp'],
              include_dirs=['/usr/local/home/ssgrant/RemoteDataStructres/rcuckoo_rdma/xxHash'],
              libraries=['xxhash'],
              extra_compile_args=['-std=c++11'],
              language='c++'
              ),
]

# extensions = [
#     Extension('chash', ['chash.pyx', 'hashpp.cpp'],
#               extra_compile_args=['-std=c++11'],
#               language='c++'
#               ),
# ]

setup(
    name='chash',
    ext_modules=cythonize(extensions, language_level=3),
    # extra_compile_args=["-w", '-g'],
)