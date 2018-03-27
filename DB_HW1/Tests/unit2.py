from Storage.Page import Page
from Storage.SlottedPage import SlottedPage
from Storage.File import StorageFile
from Storage.FileManager import FileManager
from Storage.BufferPool import BufferPool
from Catalog.Identifiers import FileId, PageId, TupleId
from Catalog.Schema import DBSchema

import sys
import unittest

# Change this to 'pageClass = SlottedPage' to test the SlottedPage class.
pageClass = Page

class Hw1PublicTests(unittest.TestCase):
  ###########################################################
  # Page Class Tests
  ###########################################################
  # Utils:
  def makeSchema(self):
    return DBSchema('employee', [('id', 'int'), ('age', 'int')])

  def makeEmployee(self, n):
    schema = self.makeSchema()
    return schema.instantiate(n, 25 + n)

  def makeEmptyPage(self):
    schema = self.makeSchema()
    pId = PageId(FileId(1), 100)
    return pageClass(pageId=pId, buffer=bytes(4096), schema=schema)

  # Tests
  ###########################################################
  # File Class Tests
  ###########################################################
  # Utils:
  def makeDB(self):
    schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
    bp = BufferPool()
    fm = FileManager(bufferPool=bp)
    bp.setFileManager(fm)
    return (bp, fm, schema)

  #def makePage(self, schema, fId, f,i):
  #  pId = PageId(fId, i)
  #  p = SlottedPage(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
  #  for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(1000)]:
  #    p.insertTuple(tup)
  #  return (pId, p)

  # Tests:
  def testFileAvailablePage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    tf = open("break.txt", 'a')
    tf.write("point 1")
    # Since we aren't adding any data, 
    # The available page shouldn't change.
    # Even as we allocate more pages
    initialPage = f.availablePage().pageIndex
    #for i in range(10):
    #  f.allocatePage()
    #  self.assertEqual(f.availablePage().pageIndex, initialPage)
    tf.write("point 2->")
    tf.write(str(initialPage))
    # Now we fill some pages to check that the available page has changed.
    for i in range(2000):
      f.insertTuple(schema.pack(self.makeEmployee(i)))
    tf.write("point 3->")
    tf.write(str(f.availablePage().pageIndex))
    self.assertNotEqual(f.availablePage().pageIndex, initialPage)
    tf.write("NOTHINGMAKESSENSE")
    filem.close()
    tf.write("point 4")
    tf.close()    

if __name__ == '__main__':
  unittest.main(argv=[sys.argv[0], '-v'])
