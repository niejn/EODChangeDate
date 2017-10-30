from distutils.core import setup
import py2exe
# import sys
# sys.setrecursionlimit(5000)
import sys
sys.setrecursionlimit(1000000)
setup(windows=["entry.py"])