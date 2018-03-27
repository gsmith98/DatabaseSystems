import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page

from bitstringmaster.bitstring import BitArray

###########################################################
# DESIGN QUESTION 1: should this inherit from PageHeader?
# If so, what methods can we reuse from the parent?
#
class SlottedPageHeader(PageHeader):
  """
  A slotted page header implementation. This should store a slot bitmap
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = SlottedPageHeader.unpack(buffer.getbuffer())
  >>> ph == ph2
  True

  ## Dirty bit tests
  >>> ph.isDirty()
  False
  >>> ph.setDirty(True)
  >>> ph.isDirty()
  True
  >>> ph.setDirty(False)
  >>> ph.isDirty()
  False

  ## Tuple count tests
  >>> ph.hasFreeTuple()
  True

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  >>> ph.nextFreeTuple() == 0
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  
  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [11, 12, ...]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True
  
  >>> ph.freeSpace() < ph.tupleSize
  True
  """

  def __init__(self, **kwargs):
    buffer=kwargs.get("buffer", None)
    self.flags           = kwargs.get("flags", b'\x00')
    self.pageCapacity = kwargs.get("pageCapacity", len(buffer))
    self.tupleSize = kwargs.get("tupleSize", None)
    self.bitmap=kwargs.get("bitmap", None)

    if buffer == None:
      raise ValueError("No backing buffer supplied for SlottedPageHeader")

    if self.bitmap == None:
      headerSizeWithoutBitmap = struct.Struct("cHHH").size
      tupleCapacity = math.floor((8*(self.pageCapacity-headerSizeWithoutBitmap))/(1+(8*self.tupleSize)))
      bString = '0b' + ('0' * tupleCapacity)
      self.bitmap = BitArray(bString)
   
    self.binrepr   = struct.Struct("cHHH" + str(math.ceil(len(self.bitmap))) + 's')
    self.size      = self.binrepr.size
    self.freeSpaceOffset = self.size
   
    buffer[0:self.size] = self.pack()
 #   super().__init__(buffer=buffer, flags=kwargs.get("flags", b'\x00'), self.tupleSize)
  
  def __eq__(self, other):
    return (    self.flags == other.flags
            and self.tupleSize == other.tupleSize
            and self.pageCapacity == other.pageCapacity
            and self.freeSpaceOffset == other.freeSpaceOffset 
            and self.bitmap == other.bitmap)

  def __hash__(self):
    return hash((self.flags, self.tupleSize, self.pageCapacity, self.freeSpaceOffset, self.bitmap))

  def headerSize(self):
    return self.size

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)

  def numTuples(self):
    return self.bitmap.count(1)

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - (self.size + (self.numTuples() * self.tupleSize))

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    return (self.numTuples() * self.tupleSize)


  # Slot operations.
  def offsetOfSlot(self, slot):
    return slot * self.tupleSize + self.size

  def hasSlot(self, slotIndex):
    return slotIndex < len(self.bitmap)

  def getSlot(self, slotIndex):
    return self.bitmap[slotIndex] == '0b1' 

  def setSlot(self, slotIndex, slot):
    if not self.hasSlot(slotIndex):
      return

    if slot == True:
      self.bitmap[slotIndex] = '0b1'
    elif slot == False:
      self.bitmap[slotIndex] = '0b0'

  def resetSlot(self, slotIndex):
    self.setSlot(slotIndex, False)

  def freeSlots(self):
    freeList = []
    for i in range(len(self.bitmap)):
      if self.bitmap[i] == '0b0':
        freeList.append(i)
    return freeList

  def usedSlots(self):
    usedList = []
    for i in range(len(self.bitmap)):
      if self.bitmap[i] == '0b1':
        usedList.append(i)
    return usedList

  # Tuple allocation operations.
  
  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    return self.freeSpace() >= self.tupleSize
    # findTuple = self.bitmap.find('0b0')
    # if findTuple == ():
    #   return False
    # else:
    #   return True

  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    #nextTuple = self.bitmap.find('0b0')

    #if nextTuple == ():
    #  return None
    
    #headerSizeWithoutBitmap = struct.Struct("cHHH").size
    #tupleCapacity = math.floor((8*(self.pageCapacity-headerSizeWithoutBitmap))/(1+(8*self.tupleSize)))
    #if nextTuple[0] >= tupleCapacity:
    #  return None

    #self.bitmap[nextTuple[0]] = '0b1'
    #return nextTuple[0]
    if self.hasFreeTuple():
      nextTuple = self.bitmap.find('0b0')
      self.bitmap[nextTuple[0]] = '0b1'
      return nextTuple[0]
    else:
      return None

  def nextTupleRange(self):
    start = self.nextFreeTuple()
    end = start + self.tupleSize
    index = (start - self.size)//self.tupleSize
    return (index, start, end)

  # Create a binary representation of a slotted page header.
  # The binary representation should include the slot contents.
  def pack(self):
    byteArray = bytearray(self.bitmap)

    packed = self.binrepr.pack(
              self.flags, self.tupleSize,
              self.freeSpaceOffset, self.pageCapacity, byteArray)
    unpacked = self.binrepr.unpack_from(packed)

    return self.binrepr.pack(
              self.flags, self.tupleSize,
              self.freeSpaceOffset, self.pageCapacity, byteArray)

  # Create a slotted page header instance from a binary representation held in the given buffer.
  @classmethod
  def unpack(cls, buffer):
    binrepr1 = struct.Struct("cHHH")
    values1 = binrepr1.unpack_from(buffer)

    headerSizeWithoutBitmap = binrepr1.size
    tupleCapacity = math.floor((8*(values1[3]-headerSizeWithoutBitmap))/(1+(8*values1[1])))

    binrepr2   = struct.Struct("cHHH" + str(math.ceil(tupleCapacity)) + 's')
    values2 = binrepr2.unpack_from(buffer)

    bString = '0b' + ('0' * tupleCapacity)
    bitmap = BitArray(bString)

    index = 0
    for bit in values2[4]:
      if index >= tupleCapacity:
        break
      if bit:
        bitmap[index] = '0b1'
      else:
        bitmap[index] = '0b0'
      index = index + 1

    if len(values2) == 5:
      return cls(buffer=buffer, flags=values2[0], tupleSize=values2[1],
                 freeSpaceOffset=values2[2], pageCapacity=values2[3], 
                 bitmap=bitmap)



######################################################
# DESIGN QUESTION 2: should this inherit from Page?
# If so, what methods can we reuse from the parent?
#
class SlottedPage(Page):
  """
  A slotted page implementation.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  A slotted page interprets the tupleIndex field in a TupleId object as
  a slot index.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()  
  >>> p.clearTuple(tId)
  
  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]
  
  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = SlottedPageHeader

  # Slotted page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a SlottedPageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a SlottedPageHeader.
  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)
    if buffer:
      BytesIO.__init__(self, buffer)
      self.pageId = kwargs.get("pageId", None)
      header      = kwargs.get("header", None)
      schema      = kwargs.get("schema", None)

      if self.pageId and header:
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")
      
    else:
      raise ValueError("No backing buffer provided to page constructor.")


  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator.
  def __iter__(self):
    iterTuple = self.header.bitmap.find('0b1')
    if iterTuple == ():
      self.iterTupleIdx = -1
    else:
      self.iterTupleIdx = iterTuple[0]
    return self

  def __next__(self):

    t = self.getTuple(TupleId(self.pageId, self.iterTupleIdx))
    
    if t:
      nextTuple = self.header.bitmap.find('0b1', self.iterTupleIdx + 1)
      if nextTuple == ():
        self.iterTupleIdx = -1        
      else:
        self.iterTupleIdx = nextTuple[0]
      return t
    else:
      raise StopIteration

  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):

    tupleIndex = tupleId.tupleIndex

    if tupleIndex < 0:
      return None

    if self.header.bitmap[tupleIndex] == '0b0':
      return None

    view = self.getbuffer()
    offset = tupleIndex * self.header.tupleSize + self.header.size
    tupleBytes = view[offset: offset + self.header.tupleSize]

    return tupleBytes

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    super().putTuple(tupleId, tupleData)

  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    bitTuple = self.header.bitmap.find('0b0')

    if bitTuple == ():
      return None

    tupleID = TupleId(self.pageId, bitTuple[0])

    view = self.getbuffer()

    offset = (bitTuple[0] * self.header.tupleSize) + self.header.size

    if self.header.pageCapacity - offset < 8:
      return None

    view[offset : offset + self.header.tupleSize] = tupleData
    self.header.bitmap[bitTuple[0]] = '0b1'

    self.header.setDirty(0b1)

    return tupleID

  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    super().clearTuple(tupleId)

  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    self.header.bitmap[tupleId.tupleIndex] = '0b0'
    self.setDirty(0b1)

  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):

    view = self.getbuffer()

    byteHeader = self.header.pack()
    view[0:self.header.size] = byteHeader 

    return(view)

    # return super().pack()

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    # return super().unpack(pageId, buffer)

    pageHeader = SlottedPageHeader.unpack(buffer)
    return SlottedPage(pageId=pageId, buffer=buffer, header=pageHeader)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
