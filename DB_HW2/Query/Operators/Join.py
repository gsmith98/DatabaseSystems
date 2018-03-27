import itertools

from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Join(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined join operator not supported")

    self.lhsPlan    = lhsPlan
    self.rhsPlan    = rhsPlan
    self.joinExpr   = kwargs.get("expr", None)
    self.joinMethod = kwargs.get("method", None)
    self.lhsSchema  = kwargs.get("lhsSchema", None if lhsPlan is None else lhsPlan.schema())
    self.rhsSchema  = kwargs.get("rhsSchema", None if rhsPlan is None else rhsPlan.schema())

    self.lhsKeySchema   = kwargs.get("lhsKeySchema", None)
    self.rhsKeySchema   = kwargs.get("rhsKeySchema", None)
    self.lhsHashFn      = kwargs.get("lhsHashFn", None)
    self.rhsHashFn      = kwargs.get("rhsHashFn", None)

    self.validateJoin()
    self.initializeSchema()
    self.initializeMethod(**kwargs)

  # Checks the join parameters.
  def validateJoin(self):
    # Valid join methods: "nested-loops", "block-nested-loops", "indexed", "hash"
    if self.joinMethod not in ["nested-loops", "block-nested-loops", "indexed", "hash"]:
      raise ValueError("Invalid join method in join operator")

    # Check all fields are valid.
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      methodParams = [self.joinExpr]

    elif self.joinMethod == "indexed":
      methodParams = [self.lhsKeySchema]

    elif self.joinMethod == "hash":
      methodParams = [self.lhsHashFn, self.lhsKeySchema, \
                      self.rhsHashFn, self.rhsKeySchema]

    requireAllValid = [self.lhsPlan, self.rhsPlan, \
                       self.joinMethod, \
                       self.lhsSchema, self.rhsSchema ] \
                       + methodParams

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete join specification, missing join operator parameter")

    # For now, we assume that the LHS and RHS schema have
    # disjoint attribute names, enforcing this here.
    for lhsAttr in self.lhsSchema.fields:
      if lhsAttr in self.rhsSchema.fields:
        raise ValueError("Invalid join inputs, overlapping schema detected")


  # Initializes the output schema for this join.
  # This is a concatenation of all fields in the lhs and rhs schema.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.lhsSchema.schema() + self.rhsSchema.schema()
    self.joinSchema = DBSchema(schema, fields)

  # Initializes any additional operator parameters based on the join method.
  def initializeMethod(self, **kwargs):
    if self.joinMethod == "indexed":
      self.indexId = kwargs.get("indexId", None)
      if self.indexId is None or self.lhsKeySchema is None:
        raise ValueError("Invalid index for use in join operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.joinSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.lhsSchema, self.rhsSchema]

  # Returns a string describing the operator type
  def operatorType(self):
    readableJoinTypes = { 'nested-loops'       : 'NL'
                        , 'block-nested-loops' : 'BNL'
                        , 'indexed'            : 'Index'
                        , 'hash'               : 'Hash' }
    return readableJoinTypes[self.joinMethod] + "Join"

  # Returns child operators if present
  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for join operator.
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


 #raise NotImplementedError

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.joinMethod == "nested-loops":
      return self.nestedLoops()

    elif self.joinMethod == "block-nested-loops":
      return self.blockNestedLoops()

    elif self.joinMethod == "indexed":
      return self.indexedNestedLoops()

    elif self.joinMethod == "hash":
      return self.hashJoin()

    else:
      raise ValueError("Invalid join method in join operator")


  ##################################
  #
  # Nested loops implementation
  #
  def nestedLoops(self):
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

        for (rPageId, rhsPage) in iter(self.rhsPlan):
          for rTuple in rhsPage:
            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            # Evaluate the join predicate, and output if we have a match.
            if eval(self.joinExpr, globals(), joinExprEnv):
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())


  ##################################
  #
  # Block nested loops implementation
  #
  # This attempts to use all the free pages in the buffer pool
  # for its block of the outer relation.

  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    pinnedPages = []

    M = bufPool.numPages()
    count = 0

    try:
      while count < (M-2):
        (pageId,pageObj) = next(pageIterator) 
        bufPool.pinPage(pageId)
        pinnedPages.append((pageId, pageObj))
        count += 1
    except StopIteration:
      pass

    return pinnedPages        

  def blockNestedLoops(self):
    lIter = iter(self.lhsPlan)
    pinnedPages = self.accessPageBlock(self.storage.bufferPool, lIter)
    while (len(pinnedPages) > 0):
      for (lPageId, lhsPage) in iter(pinnedPages):
        for lTuple in lhsPage:
          # Load the lhs once per inner loop.
          joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

          for (rPageId, rhsPage) in iter(self.rhsPlan):
            for rTuple in rhsPage:
              # Load the RHS tuple fields.
              joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

              # Evaluate the join predicate, and output if we have a match.
              if eval(self.joinExpr, globals(), joinExprEnv):
                outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                self.emitOutputTuple(self.joinSchema.pack(outputTuple))

          # No need to track anything but the last output page when in batch mode.
          if self.outputPages:
            self.outputPages = [self.outputPages[-1]]
      for (pageId, pageObj) in pinnedPages:
        self.storage.bufferPool.unpinPage(pageId)
      pinnedPages = self.accessPageBlock(self.storage.bufferPool, lIter)

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())


  ##################################
  #
  # Indexed nested loops implementation
  #
  # TODO: test
  def indexedNestedLoops(self):
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)
        joinKey = self.lhsKeySchema.pack(self.lhsSchema.project(self.lhsSchema.unpack(lTuple), self.lhsKeySchema))

        #matches is an iterator over tuple IDs
        matches = self.storage.fileMgr.lookupByIndex(self.rhsPlan.relationId(), self.indexId, joinKey)

        if not matches:
          continue

        for rTupleID in matches:
          rFile      = self.storage.fileMgr.relationFile(self.rhsPlan.relationId())[1]
          pId        = rTupleID.pageId
          rpage      = rFile.bufferPool.getPage(pId)
          rtupleData = rpage.getTuple(rTupleID)
          #unpack rtupleData?
          joinExprEnv.update(self.loadSchema(self.rhsSchema, rtupleData))
          if eval(self.joinExpr, globals(), joinExprEnv):
            outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
            self.emitOutputTuple(self.joinSchema.pack(outputTuple))
    return self.storage.pages(self.relationId())
 
    #raise NotImplementedError

  ##################################
  #
  # Hash join implementation.
  #
  def hashJoin(self):
    lRelIds = []
    rRelIds = []
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        hashExprEnv = self.loadSchema(self.lhsSchema, lTuple)
        tupleHash = eval(self.lhsHashFn, globals(), hashExprEnv)
        
        relId = str(self.id()) + "l" + str(tupleHash)
        self.storage.createRelation(relId, self.lhsSchema)
        self.storage.insertTuple(relId, lTuple)
        
        if str(tupleHash) not in lRelIds:
          lRelIds.append(str(tupleHash))

    for (rPageId, rhsPage) in iter(self.rhsPlan):
      for rTuple in rhsPage:
        hashExprEnv = self.loadSchema(self.rhsSchema, rTuple)
        tupleHash = eval(self.rhsHashFn, globals(), hashExprEnv)
        
        relId = str(self.id()) + "r" + str(tupleHash)
        self.storage.createRelation(relId, self.rhsSchema)
        self.storage.insertTuple(relId, rTuple)

        if str(tupleHash) not in rRelIds:
          rRelIds.append(str(tupleHash))

    if not self.joinExpr:
      self.joinExpr = "True" 
    for k in range(len(self.lhsKeySchema.fields)):
      self.joinExpr += " and " + self.lhsKeySchema.fields[k] + " == " + self.rhsKeySchema.fields[k] 
    for lId in lRelIds:
      if lId in rRelIds:
        ######DO BNLJ#######
    
        lIter = iter(self.storage.pages(str(self.id()) + "l" + lId))
        pinnedPages = self.accessPageBlock(self.storage.bufferPool, lIter)
        while (len(pinnedPages) > 0):
          for (lPageId, lhsPage) in iter(pinnedPages):
            for lTuple in lhsPage:
              # Load the lhs once per inner loop.
              joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

              for (rPageId, rhsPage) in iter(self.storage.pages(str(self.id()) + "r" + lId)):
                for rTuple in rhsPage:
                  # Load the RHS tuple fields.
                  joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))
 
                  # Evaluate the join predicate, and output if we have a match.
                  if eval(self.joinExpr, globals(), joinExprEnv):
                    outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                    self.emitOutputTuple(self.joinSchema.pack(outputTuple))

              # No need to track anything but the last output page when in batch mode.
              if self.outputPages:
                self.outputPages = [self.outputPages[-1]]
          for (pageId, pageObj) in pinnedPages:
            self.storage.bufferPool.unpinPage(pageId)
          pinnedPages = self.accessPageBlock(self.storage.bufferPool, lIter)

       ######END BNLJ######

    return self.storage.pages(self.relationId())


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      exprs = "(expr='" + str(self.joinExpr) + "')"

    elif self.joinMethod == "indexed":
      exprs =  "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "indexKeySchema=" + self.lhsKeySchema.toString() ]
        ))) + ")"

    elif self.joinMethod == "hash":
      exprs = "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "lhsKeySchema=" + self.lhsKeySchema.toString() ,
            "rhsKeySchema=" + self.rhsKeySchema.toString() ,
            "lhsHashFn='" + self.lhsHashFn + "'" ,
            "rhsHashFn='" + self.rhsHashFn + "'" ]
        ))) + ")"

    return super().explain() + exprs
