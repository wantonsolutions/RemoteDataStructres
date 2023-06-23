from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
from Cython.Distutils import build_ext
import numpy

import os
xx_hash_dir = './xxHash'
xx_hash_abs_dir = os.path.abspath(xx_hash_dir)
#get the absolute path of the current directory
ap = os.path.abspath(os.path.dirname(__file__))
ap=ap+"/"
print(ap)

extensions = [
    Extension('rcuckoo_wrap', ["rcuckoo_wrapper.pyx", 'hash.cpp', 'util.cpp', 'tables.cpp', 'search.cpp', 'virtual_rdma.cpp', "state_machines.cpp", "cuckoo.cpp"],
              include_dirs=[xx_hash_abs_dir],
              libraries=['xxhash'],
            #   extra_compile_args=['-std=c++2a'],
              extra_compile_args=['-std=c++2a', '-DDebug','-g'],
              language='c++'
              ),
]

# extensions = [
#     Extension('rcuckoo_rdma.hash', [ap + "hash_wrapper.pyx", ap+'hash.cpp'],
#               include_dirs=[xx_hash_abs_dir],
#               libraries=['xxhash'],
#               extra_compile_args=['-std=c++11'],
#               language='c++'
#               ),
#     Extension('rcuckoo_rdma.tables', [ap+'tables_wrapper.pyx', ap+'tables.cpp'],
#               extra_compile_args=['-std=c++11'],
#               language='c++'
#               ),
#     # Extension('csearch', ['search_wrapper.pyx', 'search.cpp'],
#     #           extra_compile_args=['-std=c++11'],
#     #           language='c++'
#     #           ),
# ]



setup(
    name='rcuckoo_wrap',
    # cmdclass={'build_ext': build_ext},
    ext_modules=cythonize(extensions, language_level=3),
    # extra_compile_args=["-w", '-g'],
)

# setup(
#     name='csearch',
#     ext_modules=cythonize(extensions, language_level=3),
#     # extra_compile_args=["-w", '-g'],
# )

#Use this setup.py if you want setup to automatically cythonize all pyx in the codeRootFolder
#To run this setup do exefile('pathToThisSetup.py')

# import os
# from distutils.core import setup
# from distutils.extension import Extension
# from Cython.Distutils import build_ext

# def scandir(dir, files=[]):
#     for file in os.listdir(dir):
#         path = os.path.join(dir, file)
#         if os.path.isfile(path) and path.endswith(".pyx"):
#             files.append(path.replace(os.path.sep, ".")[:-4])
#         elif os.path.isdir(path):
#             scandir(path, files)
#     return files


# def makeExtension(extName):
#     extPath = extName.replace(".", os.path.sep)+".pyx"
#     return Extension(
#         extName,
#         [extPath],
#         include_dirs = ['.'] #your include_dirs must contains the '.' for setup to search all the subfolder of the codeRootFolder
#         )


# extNames = scandir('.')

# extensions = [makeExtension(name) for name in extNames]

# setup(
#   name="workingCythonMultiPackageProject",
#   ext_modules=extensions,
#   cmdclass = {'build_ext': build_ext},
#   script_args = ['build_ext'],
#   options = {'build_ext':{'inplace':True, 'force':True}}
# )

# print '********CYTHON COMPLETE******'