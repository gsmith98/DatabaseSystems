from Catalog.Schema import DBSchema
from Query.Operator import Operator

class GroupBy(Operator):
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None)
    self.aggSchema   = kwargs.get("aggSchema", None)
    self.groupExpr   = kwargs.get("groupExpr", None)
    self.aggExprs    = kwargs.get("aggExprs", None)
    self.groupHashFn = kwargs.get("groupHashFn", None)

    self.validateGroupBy()
    self.initializeSchema()

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for selection operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputFinished = False
    if not self.pipelined:
      self.outputIterator = self.processAllPages()
    return self
    #raise NotImplementedError

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
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    outputSchema = DBSchema(schema,fields)

    relIds = []
    for (pageId, page) in iter(self.subPlan):
      for tpl in page:
        group = self.groupExpr(self.subSchema.unpack(tpl))
        key = self.groupHashFn((group, None))
        relId = str(self.id) + "u" + str(key)
        self.storage.createRelation(relId, self.subSchema)
        self.storage.insertTuple(relId, tpl)
        if relId not in relIds:
          relIds.append(relId)

    for rid in relIds:
      groupDict = {}
      for (pageId, page) in self.storage.pages(rid):
        for tpl in page:
          groupKey = self.groupExpr(self.subSchema.unpack(tpl))
          if groupKey not in groupDict:
            groupDict[groupKey] = []
            for trio in self.aggExprs:
              groupDict[groupKey].append(trio[0])
              
          for i in range(len(self.aggExprs)):
            groupDict[groupKey][i] = self.aggExprs[i][1](groupDict[groupKey][i], self.subSchema.unpack(tpl))

      for key in groupDict:
        for i in range(len(self.aggExprs)):
          groupDict[key][i] = self.aggExprs[i][2](groupDict[key][i])
      
      for key in groupDict:
        outTuple = outputSchema.instantiate(key, *[f for f in groupDict[key]])
        self.emitOutputTuple(self.outputSchema.pack(outTuple))
    return self.storage.pages(self.relationId())







    #raise NotImplementedError

  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"
