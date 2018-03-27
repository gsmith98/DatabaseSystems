import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)


    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?
    self.freeList     = []
    for i in range(self.poolSize // self.pageSize):
      self.freeList.append(i * self.pageSize)

    self.pageDict = OrderedDict()


  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize // self.pageSize)

  def numFreePages(self):
    tf = open("num.txt", "w")
    tf.write(str(self.numPages()) + ", " + str(len(self.freeList)))
    tf.close()
    return len(self.freeList)
    # raise NotImplementedError

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()

  # helper methods
  def pageFromBuffer(self, pageId):
    if not self.hasPage(pageId):
      return None

    view = self.pool.getbuffer()

    offset = self.pageDict[pageId]
    pageBuffer = view[offset : offset + self.pageSize]

    return pageBuffer

  def updateBuffer(self, pageId, pageBuffer):
    if self.hasPage(pageId):
      offset = self.pageDict[pageId]
      self.pageDict.pop(pageId)
    elif len(self.freeList) > 0:
      offset = self.freeList.pop(0)
    else:
      self.evictPage()
      offset = self.freeList.pop(0)
      
    self.pageDict[pageId] = offset

    view = self.pool.getbuffer()
    view[offset : offset + self.pageSize] = pageBuffer

  # Buffer pool operations

  def hasPage(self, pageId):
    return pageId in self.pageDict
    # raise NotImplementedError
  
  def getPage(self, pageId):
    if self.hasPage(pageId):
      pageBuffer = self.pageFromBuffer(pageId)
      self.updateBuffer(pageId, pageBuffer)

      rFile = self.fileMgr.fileMap.get(pageId.fileId, None)
      page = rFile.pageClass().unpack(pageId, pageBuffer)

      return page
 
    if len(self.freeList) == 0:
      #evict
      self.evictPage()

    offset = self.freeList.pop(0)

    view = self.pool.getbuffer()
    pageBuffer = view[offset : offset + self.pageSize]

    page = self.fileMgr.readPage(pageId, pageBuffer)
    # self.updateBuffer(pageId, pagebuffer)

    self.pageDict[pageId] = offset 
    view[offset : offset + self.pageSize] = page.pack()

    # self.pageDict[pageId] = offset
    # view[offset : offset + self.pageSize] = pageBuffer


    return page
    # raise NotImplementedError

  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    offset = self.pageDict.pop(pageId)
    self.freeList.append(offset)
    # raise NotImplementedError

  def flushPage(self, pageId):

    rFile = self.fileMgr.fileMap.get(pageId.fileId, None)
    pageBuffer = self.pageFromBuffer(pageId)
    page = rFile.pageClass().unpack(pageId, pageBuffer)

    if page.header.isDirty():
      rFile.writePage(page)
    # raise NotImplementedError

  # Evict using LRU policy. 
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  # returns offset evicted
  def evictPage(self):
    pageId = next(iter(self.pageDict))
    self.flushPage(pageId)
    tup = self.pageDict.popitem(last=False)
    pageId = tup[0]  # should be the same as it just was
    offset = tup[1]
    self.freeList.append(offset)
    return offset
    # raise NotImplementedError

  # Flushes all dirty pages
  def clear(self):
    for pId in self.pageDict.keys():
      self.flushPage(pId)

    # raise NotImplementedError


if __name__ == "__main__":
    import doctest
    doctest.testmod()
