from Storage.Page import Page
from Storage.SlottedPage import SlottedPage
from Storage.File import StorageFile
from Storage.FileManager import FileManager
from Storage.BufferPool import BufferPool
from Catalog.Identifiers import FileId, PageId, TupleId
from Catalog.Schema import DBSchema

import sys
import unittest

# A schema to work with
schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])

# Initialize a bufferpool and filemanager
def makeDB():
  bp = BufferPool()
  fm = FileManager(bufferPool=bp)
  bp.setFileManager(fm)
  return (bp, fm)
 
# Make an employee 
def makeEmployee(n):
  return schema.instantiate(n, 25 + n)

if __name__ == "__main__":
  print("Creating")
  (bp, fm) = makeDB()
  fm.createRelation(schema.name, schema)
  (fId, f) = fm.relationFile(schema.name)

  print("Populating")
  for i in range(100):
    f.insertTuple(schema.pack(makeEmployee(i)))
  print("Num tuples: %s" % f.numTuples())
 
  print("Deleting") 
  fm.close()
  del bp
  del fm

  print("Restoring")
  (bp, fm) = makeDB()
  (fId, f) = fm.relationFile(schema.name)
  print("Num tuples: %s" % f.numTuples())
  fm.close()
