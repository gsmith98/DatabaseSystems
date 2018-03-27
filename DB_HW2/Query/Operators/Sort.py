from Catalog.Schema import DBSchema
from Query.Operator import Operator

# Operator for External Sort
class Sort(Operator):

  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)
    self.subPlan     = subPlan
    self.sortKeyFn   = kwargs.get("sortKeyFn", None)
    self.sortKeyDesc = kwargs.get("sortKeyDesc", None)

    if self.sortKeyFn is None or self.sortKeyDesc is None:
      raise ValueError("No sort key extractor provided to a sort operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.subPlan.schema()

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "Sort"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]


  # Iterator abstraction for external sort operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputFinished = False
    if not self.pipelined:
      self.outputIterator = self.processAllPages()
    return self

  def __next__(self):
    if self.pipelined:
      while not(self.inputFinished or self.isOutputPageReady()):
        try:
          pageId, page = next(self.inputIterator)
          self.processInputPage(pageId, page)
        except StopIteration:
          self.inputFinished = True

      return self.outputPage()

    else:
      return next(self.outputIterator)


  # Page processing and control methods

  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    pinnedPages = []

    M = bufPool.numPages()
    count = 0

    try:
      while count < (M): #not M-2 anymore
        (pageId,pageObj) = next(pageIterator) 
        bufPool.pinPage(pageId)
        pinnedPages.append((pageId, pageObj))
        count += 1
    except StopIteration:
      pass

    return pinnedPages        


  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise NotImplementedError

  # Set-at-a-time operator processing
  def processAllPages(self):
    sIter = iter(self.subPlan)
    pinnedPages = self.accessPageBlock(self.storage.bufferPool, sIter)
    run = 0
    runList = []
    while (len(pinnedPages) > 0):
      pytSortList = []
      for (sPageId, sPage) in iter(pinnedPages):
        for sTuple in sPage:
          pytSortList.append(self.schema().unpack(sTuple))
      doneBlockList = sorted(pytSortList, key=self.sortKeyFn, reverse=True)
      relId = str(self.id()) + "run" + str(run)
      self.storage.createRelation(relId, self.schema())
      for tup in doneBlockList:
        self.storage.insertTuple(relId, tup)

      for (pageId, pageObj) in pinnedPages:
        self.storage.bufferPool.unpinPage(pageId)
      pinnedPages = self.accessPageBlock(self.storage.bufferPool, sIter)
      runList.append(relId)
      run += 1
    
  
    # n-way merge startegy: Have list of n iterators over n runs to be merged
    # have list of n storage Tuples currently being compared
    # repeatedly: find min in comp list, write it, and replace with (None, run#)
    # when comparing if encounter a None, pop from run# to replace
    # when min is found, write it
    # when run is emptied, remove it from list
    # continue until run list is empty 

    runIterList = []
    for rel in runList:
      runIterList.append(self.storage.tuples(rel)) # experiencing an issue where iterator is immediately empty

    comparisonList = []
    for i in range(len(runIterList)):
      comparisonList.append(None)

    while(len(runIterList) > 0):
      #tf = open("sorttest", "a")
      #tf.write("inside loop\n")
      #tf.close()

      minm = None
      minidx = -1
      i = 0
      while i < len(comparisonList):
        #tf = open("sorttest", "a")
        #tf.write("inside inner loop\n")
        #tf.close()

        tupTup = comparisonList[i]

        if tupTup == None:
          try:
            tupTup = next(runIterList[i]) #experiencing an issue where iterator is immediately empty
            #tf = open("sorttest", "a")
            #tf.write("not exception\n")
            #tf.close()
            comparisonlist[i] = tupTup
          except StopIteration:
            #tf = open("sorttest", "a")
            #tf.write("inside stop clause\n")
            #tf.close()
            runIterList.pop(i)
            comparisonList.pop(i)
            break #without incrementing i because list indices just moved down
            
        if minm:
          lastminm = minm
          minm = min([minm, tupTup], key=self.sortKeyFn)
          if minm != lastminm:
            minidx = i
        else:
          minm = tupTup
          minidx = i

        i += 1

      if minm:
        self.emitOutputTuple(minm.pack())
        comparisonList[minidx] = None

    return self.storage.pages(self.relationId())

      
   
  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(sortKeyDesc='" + str(self.sortKeyDesc) + "')"
