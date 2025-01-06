# python set_up.py build_ext --inplace
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize
import numpy

# 创建 Extension 对象
ext = Extension("sim_engine",
                sources=["sim_engine.pyx"],
                include_dirs=[numpy.get_include()])

# 调用 cythonize 函数并传递 Extension 对象
setup(
    ext_modules=cythonize(ext)
)
