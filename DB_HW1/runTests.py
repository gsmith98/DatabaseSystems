#from Database                import Database
from Utils.WorkloadGenerator import WorkloadGenerator
from Storage.File            import StorageFile
from Storage.Page            import Page
from Storage.SlottedPage     import SlottedPage
import sys

# Path to the folder containing csvs (on ugrad cluster)
dataDir = '/home/cs416/datasets/tpch-sf0.01/'

# Pick a page class, page size, scale factor, and workload mode:
#if int(sys.argv[1]) == 0:   # Page type
#  StorageFile.defaultPageClass = Page
#  sys.stdout.write("Page, ")
#elif int(sys.argv[1]) == 1: # SlottedPage type
#  StorageFile.defaultPageClass = SlottedPage
#  sys.stdout.write("SlottedPage, ")
#
#pageSize = int(sys.argv[2])                       # Page size
#scaleFactor = float(sys.argv[3])                     # Datasize factor
#workloadMode = int(sys.argv[4])                      # Work mode
#
#sys.stdout.write(sys.argv[2] + ", " + sys.argv[3] + ", " + sys.argv[4] + ", ")

StorageFile.defaultPageClass = Page
for pageSize in [4096, 32768]:
  for scaleFactor in [0.2, 0.4, 0.6, 0.8, 1.0]:
    for workloadMode in [1, 2, 3, 4]:
      try:
        sys.stdout.write("Page, " + str(pageSize) + ", " + str(scaleFactor) + ", " + str(workloadMode) + ", ")
        wg = WorkloadGenerator()
        wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)
      except:
        sys.stdout.write("Error, Error\n")

StorageFile.defaultPageClass = SlottedPage
for pageSize in [4096, 32768]:
  for scaleFactor in [0.2, 0.4, 0.6, 0.8, 1.0]:
    for workloadMode in [1, 2, 3, 4]:
      try:
        sys.stdout.write("SlottedPage, " + str(pageSize) + ", " + str(scaleFactor) + ", " + str(workloadMode) + ", ")
        wg = WorkloadGenerator()
        wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)
      except:
        sys.stdout.write("Error, Error\n")





