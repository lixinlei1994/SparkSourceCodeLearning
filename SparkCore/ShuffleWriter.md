# ShuffleWriter
## SortShuffleWriter
首先看一下write()
```java
/** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // 判断是否需要使用Map端的合并
    // 上下两种情况，就是下面的不使用聚合也不使用排序
    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      /*
      * 不会向sorter传入聚合或者排序，因为我们不关心在每个partition中key是否是有序
      * 如果正在执行的是sortByKey,那么排序操作会在reduce端实现
      * */
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    // sorter中保存了所有产生的溢写文件
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }
```
接下来看一下insertAll(),调用的是ExternalSorter的
```java
def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    // 判断是否需要进行map端的合并操作
    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      // 来一条数据我们首先使用Map在内存中进行更新
      // 下面两个是两个方法
      // 对应rdd.aggregatorByKey的 seqOp 参数
      val mergeValue = aggregator.get.mergeValue
      // 对应rdd.aggregatorByKey的zeroValue参数，利用zeroValue来创建Combiner
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      // 判断当前key是否之前出现过，从而决定是更新还是创建一个新的combiner
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        // 检查spill的频率
        addElementsRead()
        // 获取下一条记录
        kv = records.next()

        // ((ppartition,key),update方法)
        map.changeValue((getPartition(kv._1), kv._1), update)
        // 判断是否需要将内存中的数据溢写到磁盘
        maybeSpillCollection(usingMap = true)
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
      }
    }
  }

```
接下来看一下SizeTrackingAppendOnlyMap的changeValue()
```java
override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    // 针对当前key，使用新的value和旧的value来更新map中的value
    val newValue = super.changeValue(key, updateFunc)
    // 如果更新次数达到一定程度，那么就进行一次抽样，用来估计集合当前的大小
    super.afterUpdate()
    newValue
  }
```
接着看父类的changeValuue,AppendOnlyMap
这里的逻辑很简单，就是针对每个key,这里的key的类型是(partitonId,key)
针对这个key计算一次hash值然后调用rehash再进行与运算，相当于计算一个在数组中的位置
data(2*pos)得到的是key,data(2*pos+1)得到的是value
如果该位置上没有元素，那么就直接插入
如果该位置上有元素并且key相同，那么就更新value
如果该位置上有元素但是key不相同，那么就向前前进一个位置继续判断
```java
/**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else if (k.eq(curKey) || k.equals(curKey)) {
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }
```
接下来看maybeSpillCollection()

```java
/**
   * Spill the current in-memory collection to disk if needed.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }
```
```java
protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    // 溢写到磁盘的条件
    /*
    * 已读取数据是32的倍数，当前占用的内存量大于设定的阈值
    首先会请求增大容量，如果无法获得足够多的容量，就会发生溢写
    * */
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      // 获取额外的内存
      // 尝试获得amountToRequest大小的内存
      val granted = acquireMemory(amountToRequest)
      // 更新阈值为2*currentMemory
      myMemoryThreshold += granted
      // 如果分配的内存量不够，那么我们就需要进行溢写了
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    // 或者读取的数据量大于了设置的强制溢写的阈值
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) {
      _spillCount += 1
      // 打印溢写信息
      logSpillage(currentMemory)
      spill(collection)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      releaseMemory()
    }
    shouldSpill
  }
```
ExternalSorter的spill()
```java
/*
  * 将内存中的集合数据溢写到一个排序文件中，方便之后的合并
  * 将这些文件添加 到spilledFiles中，方便之后的查找
  * */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // 首先进行排序，首先按照partitionId进行排序然后使用key来进行排序
    // 这里的comparator是：如果操作需要排序或者合并，那么会返回用户定义的comparator，如果用户本身没有
    // 定义comparator，那么会生成一个默认的
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    spills += spillFile
  }
  // 默认提供的keyComparator实际上比较的是key的hashcode
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })
  /*
  * 遍历数据，并且将数据写出而不是返回
  * 数据返回的顺序是首先按照partitionId，然后是comparator的返回值进行排序
  * 可能会破坏集合
  * */
  def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
    : WritablePartitionedIterator = {
    val it = partitionedDestructiveSortedIterator(keyComparator)
    new WritablePartitionedIterator {
      private[this] var cur = if (it.hasNext) it.next() else null

      def writeNext(writer: DiskBlockObjectWriter): Unit = {
        writer.write(cur._1._2, cur._2)
        cur = if (it.hasNext) it.next() else null
      }

      def hasNext(): Boolean = cur != null

      def nextPartition(): Int = cur._1._1
    }
  }
  /**
   * A comparator for (Int, K) pairs that orders them by only their partition ID.
   */
  def partitionComparator[K]: Comparator[(Int, K)] = new Comparator[(Int, K)] {
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      a._1 - b._1
    }
  }
  /**
   * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering.
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] = {
    new Comparator[(Int, K)] {
      override def compare(a: (Int, K), b: (Int, K)): Int = {
        val partitionDiff = a._1 - b._1
        if (partitionDiff != 0) {
          partitionDiff
        } else {
          keyComparator.compare(a._2, b._2)
        }
      }
    }
  }
  // 下面是map中的实现
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    // 首先对传入的keyComparator调用partitionKeyComparator进行处理
    // partitionComparator实际上做的就是返回一个二元组的比较器
    // 该比较器首先按照第一维排序，然后使用传入的keyComparator对第二维进行排序
    // 如果没有定义keyComparator，那么就只按照partition进行排序
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    destructiveSortedIterator(comparator)
  }
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    // 首先将hashTable底层的数组中的元素全部移动到最左边
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))

    // 对数组先按照partition排序，再按照key排序
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)

    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }


```
有上面的代码可以看出，map在进行存储时是按照hashTable的方式进行存储的，当需要写入到磁盘上时，会使用一个会破坏原有数组结构的迭代器，将数据按照先parititon后key的排序顺序返回
下面再看一下如何将内存中的数据溢写到磁盘
```java
/**
   * Spill contents of in-memory iterator to a temporary file on disk.
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator)
      : SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    // blockId的名称是temp_shuffle_随机数
    // 该方法就是在本地创建了文件
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    // 获得了一个可以直接向block写入数据的writer
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        // 获取当前记录的partition
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        // 将数据写到磁盘文件中
        inMemoryIterator.writeNext(writer)
        // 属于当前partition的记录个数加一
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (success) {
        writer.close()
      } else {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    // SpilledFile中保存了写入的文件，bblockId以及每个partition中记录的个数
    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }
```
下面再看一下每次changeValue之后所做的takeSample操作
当更新次数等于takeSample之前设置的nextSampleNum时，就会执行takeSample操作，主要是执行takeSample非常耗时
```java
// 作用就是生成下一次进行采样的更新次数
private def takeSample(): Unit = {
    // Sample是一个case class
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    // 只保存最近的两次sample结果
    if (samples.size > 2) {
      samples.dequeue()
    }
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: tail =>
        // 前后两次采样 集合大小差/更新次数差
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    // 主要设置这两个属性
    bytesPerUpdate = math.max(0, bytesDelta)
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }
// 调用完上面的操作之后会执行下面的操作
private def maybeSpillCollection(usingMap: Boolean): Unit = {
  var estimatedSize = 0L
  if (usingMap) {
    estimatedSize = map.estimateSize()
    if (maybeSpill(map, estimatedSize)) {
      map = new PartitionedAppendOnlyMap[K, C]
    }
  } else {
    estimatedSize = buffer.estimateSize()
    if (maybeSpill(buffer, estimatedSize)) {
      buffer = new PartitionedPairBuffer[K, C]
    }
  }

  if (estimatedSize > _peakMemoryUsedBytes) {
    _peakMemoryUsedBytes = estimatedSize
  }
}
def estimateSize(): Long = {
    assert(samples.nonEmpty)
    // 使用刚才在takeSample中设置的属性
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    // 使用预估的增加容量加上上次的容量作为当前的估计容量
    // 之所以不每次都执行SizeEstimator.estimate是因为该函数执行慢
    // 而我们每写入一条记录就需要获得当前集合的大小
    (samples.last.size + extrapolatedDelta).toLong
  }
```
上面解析完了集合大小估计，记录的插入，溢写到磁盘的过程
看一下DiskBlockManager中的创建的操作
下面是用来生成存放溢写文件的步骤
最终每个溢写文件保存的路径
spark.local.dir（其中一个文件）/spark.diskStore.subDirectories指定个数文件夹中的一个/temp_shuffle_随机数
```java

/** Produces a unique block id and File suitable for storing shuffled intermediate results. */
def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
  // temp_shuffle_随机数
  var blockId = new TempShuffleBlockId(UUID.randomUUID())
  while (getFile(blockId).exists()) {
    blockId = new TempShuffleBlockId(UUID.randomUUID())
  }
  (blockId, getFile(blockId))
}
// 文件名称如下spark.local.dir（其中一个文件）/spark.diskStore.subDirectories指定个数文件夹中的一个temp_shuffle_随机数
def getFile(filename: String): File = {
  // Figure out which local directory it hashes to, and which subdirectory in that
  // 对temp_shuffle_随机数计算hash
  val hash = Utils.nonNegativeHash(filename)
  // 根据文件名进行hash，判断放在哪个spark.local.dir的文件中
  val dirId = hash % localDirs.length
  // 判断在哪个子文件夹中
  val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

  // Create the subdirectory if it doesn't already exist
  // 如果子文件夹不存在就创建
  val subDir = subDirs(dirId).synchronized {
    val old = subDirs(dirId)(subDirId)
    if (old != null) {
      old
    } else {
      val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
      if (!newDir.exists() && !newDir.mkdir()) {
        throw new IOException(s"Failed to create local dir in $newDir.")
      }
      subDirs(dirId)(subDirId) = newDir
      newDir
    }
  }

  new File(subDir, filename)
}
/* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
// localDirs有多个
// spark.local.dir用来指定用来保存map out的文件以及存储在磁盘上的rdd
// 在spark.local.dir下面创建指定个数的子文件夹
private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)
```
下面介绍合并的过程
看write()下列代码
```java
// 获取一个文件夹
// spark.local.dir（其中一个文件）/spark.diskStore.subDirectories指定个数文件夹中的一个/"shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
// 创建一个临时文件
// ouput后面加上一个随机数
val tmp = Utils.tempFileWith(output)
try {
  // ShuffleBlockId是一个case class
  // "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
  val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
  // 将内存中的数据和溢写文件中的数据进行合并，返回每个partiton数据迭代器，将数据写入到tmp文件中，然后返回每个partitoin的记录个数
  // 完成了最终data文件的写入
  val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
  // 写入索引文件
  shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
  mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
} finally {
  if (tmp.exists() && !tmp.delete()) {
    logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
  }
}
/**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
def writePartitionedFile(
  blockId: BlockId,
  outputFile: File): Array[Long] = {

// Track location of each range in the output file
val lengths = new Array[Long](numPartitions)
val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
  context.taskMetrics().shuffleWriteMetrics)

if (spills.isEmpty) {
  // Case where we only have in-memory data
  val collection = if (aggregator.isDefined) map else buffer
  val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
  while (it.hasNext) {
    val partitionId = it.nextPartition()
    while (it.hasNext && it.nextPartition() == partitionId) {
      it.writeNext(writer)
    }
    val segment = writer.commitAndGet()
    lengths(partitionId) = segment.length
  }
} else {
  // We must perform merge-sort; get an iterator by partition and write everything directly.
  // 使用merge-sort，将分散到多个溢写文件中的数据进行排序
  for ((id, elements) <- this.partitionedIterator) {
    if (elements.hasNext) {
      for (elem <- elements) {
        writer.write(elem._1, elem._2)
      }
      val segment = writer.commitAndGet()
      lengths(id) = segment.length
    }
  }
}

writer.close()
context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

lengths
}
```
partitionedIterator实际会调用merge,在调用的时候，已经将内存中的数据按照partitionId进行重排了
```java
/*
  * 合并多个有序文件，以及内存中缓存的数据
  * 返回值是一个key-value 的迭代器，kkey是partitionId value是另外第一个迭代器，是属于该partitionId的数据
  针对每个partition生成了一个迭代器
  * */
private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
    : Iterator[(Int, Iterator[Product2[K, C]])] = {
  // 为每个spillFile创建一个读取器
  val readers = spills.map(new SpillReader(_))
  val inMemBuffered = inMemory.buffered
  (0 until numPartitions).iterator.map { p =>
    // p代表当前的partitionId
    // 获取内存中属于当前partiton的数据
    val inMemIterator = new IteratorForPartition(p, inMemBuffered)
    // 将各个文件中的数据和内存中的数据进行合并
    val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
    // 根据是否排序是否合并，返回不同的结果
    if (aggregator.isDefined) {
      // Perform partial aggregation across partitions
      // 带有聚合的merge
      (p, mergeWithAggregation(
        iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
    } else if (ordering.isDefined) {
      // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
      // sort the elements without trying to merge them
      (p, mergeSort(iterators, ordering.get))
    } else {
      (p, iterators.iterator.flatten)
    }
  }
}
```
```java
def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    // 创建一个索引文件
    val indexFile = getIndexFile(shuffleId, mapId)
    // 创建一个索引文件的临时文件
    val indexTmp = Utils.tempFileWith(indexFile)
    try {
      // 获得之前已经生成并且合并成功的数据文件
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      // 每个executor一个IndexShuffleBlockResolver，因此需要进行同步
      synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          // 在这里将索引信息写入到索引文件中
          val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
          Utils.tryWithSafeFinally {
            // We take in lengths of each block, need to convert it to offsets.
            var offset = 0L
            out.writeLong(offset)
            for (length <- lengths) {
              offset += length
              out.writeLong(offset)
            }
          } {
            out.close()
          }


          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }

          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }
```
最终生成的mapStatus会随着executor向driver发送的状态更新信息发送给driver
