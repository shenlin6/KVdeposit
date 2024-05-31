#      使用 bitcask 模型实现兼容 Redis 的 KV 储存项目





# 引言：

本次考核要求兼容**Redis**,实现KV存储。KV存储，简单的来说就是由Key和Value组成的一个储存模式。KV存储在**数据库缓存**、**分布式锁**等领域有着广泛的应用。 其设计思想不算复杂，但想要设计出一款比较高效，性能较好的KV储存器，其中有很多细节并不非常简单😁😁😁。

提到**Redis**,我们都知道它是一款性能非常好,非常受开发者欢迎的KV存储产品。但它也并不是完美的，Redis是一款基于内存的**KV存储器**，它运用了一些持久化数据的策略，但从本质上来说Redis还是面向内存设计的KV存储器，因此Redis的性能不言而喻。**有得必有失**，这种设计方式并不是完美的，即使Redis有一些关于持久化的方式，但内存中的数据是**易失性**的，所以Redis对于数据的持久性支持不够。

所以我就想与Redis设计方式有所不同，在简单的框架下实现一个面向磁盘的、数据可持久性能好的一款KV存储器，并且在**保证数据持久化**的同时，性能也完全足够用户使用。

在我查看一些资料，也根据自己之前用C++实现KV存储得经验，目前主流的数据存储模型分为两种：**B+Tree**和**LSM Tree**（“日志结构合并树），我



































# bitcask存储引擎对用户提供的接口

为了方便用户进行储存操作或者读取操作，我们可以为用户提供一下几个接口：

## 1. Get(Key) ：						  	通过 Key 获取存储的 value



## 2. Put(Key, Value) ：				 存储 key 和 value

## 3. Delete(Key) ： 	  				 删除一个 key

## 4. List_keys() ： 						 获取全部的 key

## 5. Fold(Fun) : 			  			    遍历所有的数据，执行函数 Fun

## 6. Merge(Directory Name) :   清理无效数据

## 7. Sync() :					 				将所有内核缓冲区的写入持久化到磁盘中

## 8. Close() : 								   关闭数据库





# 关于索引迭代器

我们上面为用户提供的接口中，有的需要遍历所有的数据，这时候我们内部需要定义一个**迭代器**，并且让这个迭代器实现顺序遍历、逆序遍历、跳跃遍历这三个功能，方便用户查找数据。

为了实现不同索引类型：B树或者LSM Tree都可以调用迭代器，我们将迭代器**接口抽象**出来

在遍历的时候，我们定义一个函数，让用户进行选择，默认返回true，当用户选择**返回false的时候，迭代结束**

迭代器的定义如下:

## 1. 程序内部的迭代器

### Key：当前遍历位置的 Key 数据

### 



### Value: 当前遍历位置的 Value 数据 





### Next:跳转到下一个 key





### Seek：根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历

## 

如果**正序遍历**:查找第一个大于等于传入Key的目标Key，并从这个位置开始继续遍历。

如果**逆序遍历**:查找第一个小于等于传入Key的目标Key，并从这个位置开始继续遍历。



二分法实现通过 Key 查找到对应的 index

```
if bi.reverse {

​    // bi.curindex 每次都在查找到的Index处，然后从这个位置开始遍历

​    bi.curindex = sort.Search(len(bi.values), func(i int) bool {

​      return bytes.Compare(bi.values[i].key, key) <= 0

​    })

  } else { //正序则实现原理相反

​    bi.curindex = sort.Search(len(bi.values), func(i int) bool {

​      return bytes.Compare(bi.values[i].key, key) >= 0

​    })

  }


```



### Rewind：重新回到迭代器的起点，即第一个数据

直接将 index 设置为 0 



### Valid：是否有效，即是否已经遍历完了所有的 key，用于退出遍历





### Close：关闭迭代器，释放相应资源



## 2. 迭代器的配置项（面向用户的迭代器）

为了让用户高效快速的查找到对应的数据，我们可以在用户使用迭代器查找数据的时候，提供一些**配置选项**，根据上面提供的接口，我们可以提供的选项主要包括：1. **根据文件的前缀查找文件**（因为我们默认所有文件的后缀都为 .data，这里不提供后缀选项），**默认为空**2. **选择遍历顺序**（正序或者逆序），**默认为正序**

我们同样也需要提供几个面向用户的 KV  操作接口，具体的接口**必须与程序内部的接口一致**（上面刚提到的），否则用户操作会崩掉。

**注意点 1：**我们在实例化面向用户的结构体变量的时候需要将其设置为 DB 的方法，因为我们此时需要让用户自行对数据文件进行操作。然后我们在这个方法中**调用程序内部的索引迭代器**，即可**完成用户和程序实际的连接**



![image-20240529172321432](C:\Users\shone\AppData\Roaming\Typora\typora-user-images\image-20240529172321432.png)

![image-20240529171945844](C:\Users\shone\AppData\Roaming\Typora\typora-user-images\image-20240529171945844.png)

**注意点 2**：我们**面对用户**实现的 Value 函数，并**不能**向其他函数**直接调用程序内部索引迭代器**的 Value 函数，因为我们在程序内部的索引迭代器需要拿到具体的索引信息，因此，我们**通过索引迭代器**的 Value 函数**拿到对应的索引位置**，还需要套一层逻辑处理，保证**用户**能够**拿到真实存入的 Value** 。所以我们**不能**像内部索引迭代器一样**返回一个索引信息**，而是返回 [ ]byte，即**具体的内容**

![image-20240529175410296](C:\Users\shone\AppData\Roaming\Typora\typora-user-images\image-20240529175410296.png)



同时因为我们为用户提供了配置项，我们也需要为用户提供一个



## 3.具体实现中的一些细节

### 1. 自定义迭代器可能导致数组的急速膨胀

因为 btree 提供的迭代器的数据结构不能满足我们实现所有的需求，因此我们必须自定义一个迭代器，这就导致了我们必须取出数据保存在一个数组当中，就这可能导致数组的**急速膨胀**，但这是可接受的。我们可以在发布功能之后根据用户量来决定是否需要添加**监控器**来监视我们数据是否达到一个影响性能的**阈值**。

但如果后面我们继续**添加一些索引的数据类型**，如果可以满足我们的实现需求，我们可以不用自定义迭代器来避免数据膨胀

```
func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {

  var index int //数组的索引
  values := make([]*Item, tree.Len())

  //将所有数据存放到 savevalues 数组中
  savevalues := func(bit btree.Item) bool {
​    values[index] = bit.(*Item)

​    index++

​    return true //表示一直向下遍历
  }

  if reverse {
​    //降序遍历，Descend 中需要传入一个自定义的函数,对于每个 key 按照顺序执行自定义函数的逻辑

​    tree.Descend(savevalues)

  }

  // 使用btree包里的方法,升序遍历数据
  tree.Ascend(savevalues)

  return &btreeIterator{
curindex: 0,
		reverse:  reverse,
		values:   values,
  }

}
```

### 2. 用户指定前缀情况的处理

我们需要为用户配置一些快速索引的配置项，其中，让用户来指定文件的前缀，从而快速找到用户需要的数据文件按就是其中之一。

逻辑处理主要为**死循环+跳出**来实现，如下:



当我们实现这个需求的时候，有以下几个细节需要处理：

1. 当我们读到一个文件的**前缀部分不等于用户指定前缀**的时候，直接**跳过**一个索引，反之则停止跳转，读取信息。
2. 我们需要避免当**前缀部分的长度**，**大于所遍历到的文件的长度**的时候，我们还在遍历这个文件，需要直接跳过以**提高性能**。

3. 我们需要处理的不只是用户提供的前缀长度完全等于数据文件名称的前缀长度，而是**二者的字典序完全一致**

4. 我们并不能只把我们上面实现的 Next 这个接口中加入前缀跳转的逻辑，还**同样需要**在 **Seek** 和 **Rewind** 接口中**添加前缀跳转的逻辑**

   具体实现如下:

```
// skipOne 判断Key是否带有用户需要用于过滤的前缀，不符合前缀的直接跳过一个索引

func (it *Iterator) skipOne() {

  prefixLen := len(it.options.Prefix)

  //如果用户没有指定 Prefix 的话，不用做处理

  if prefixLen == 0 {

​    return

  }

  //循环遍历所有的索引，来查找符合用户指定前缀的文件(处理方法：死循环+跳出)

  for ; it.indexIterator.Valid(); it.indexIterator.Next() {

​    key := it.indexIterator.Key()

​    //如果前缀小于等于key的长度，并且这两个前缀字典顺序完全一致时，就说明找到了用户需要的文件

​    if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {

​      break

​    }

  }

}
```



# 批量写入操作

我们将储存器交给用户使用的时候，绝大多数时间不止一个用户使用，这就需要我们考虑多个读写操作的事务性，而读操作我们之前已经添加了**读锁**。那我们现在需要把多个用户**一次性写入的数据包装为一整个事务**，保证**ACID**性质，避免用户写完之后获取数据时,出现**脏读**,**只能读一次**,**幻读**等情况。因为，我们的**逻辑核心**就在于**实现批量操作的事务性**。

## 具体逻辑处理

对于多个用户的写入操作，我们首先不直接写入磁盘，因为直接写入磁盘可能会出现多个事务一起进行导致上面提到的**并发问题**。我们首先给所有用户的写入操作加入一个**序列号**，将其看作**事务的先后序列**，根据写入时间顺序，对这个序列号进行**严格递增处理**。并且将这个序列号交给 **LogRecord** 内部来处理，保证用户的**一条 LogRecord 对应一个序列号**。对于每一个批次的事务，我们在 **LogRecord** 的最后用一个事务完成键**标记它是否提交**，如果没有提交（写入数据出现的**异常**），那么在整个序列号中，**这个事务不随着整个序列号批次提交**，我们后续使用 **merge** **清理**这些异常数据。



## 关于配置项

我们提供两个配置项：**最大缓存量**（程序内部自定义）和是否需要**持久化**（用户选择）

最大缓存量的存在时为了避免我们缓存数据太多，对性能产生影响，也可以避免内存溢出的现象

**配置的代码如下**

```
// WriteBatchOptions 批量写的配置项（用户自己配置）

type WriteBatchOptions struct {

  // 一个批次当中最大的数据量

  MaxBatchNum uint

  // 提交事务的时候是否 sync 持久化

  SyncWrites bool

}
```



## 具体代码实现

**批量原子写的结构体**

```
/ WriteBatch 原子批量写入数据，保证原子性

type WriteBatch struct {

  mu       *sync.Mutex //互斥锁

  db       *DB

  options    WriteBatchOptions      // 用户自己的配置项

  pendingWrites map[string]*data.LogRecord // 缓存用户写入，暂时不提交，保证并发安全

}
```



**我们需要为用户提供三个方法:** 

### Put

批量操作的写数据的接口

**细节处理：**

我们并**不直接写入磁盘**，而是将这条带着 K V 的数据写入LogRecord，然后先**缓存****pendingWrites**中



### Delete

删除数据的接口

**细节处理：**

**1. 如果用户要删除的数据不存在(或在缓存中**，我们需要直接删除缓存的数据，并且返回

**2**. **我们在删除数据的同时**，需要**修改**这条LogRecord的**Type**类型，改为为**已删除**类型





### Commit （核心）

将批量写入的缓存数据全部写入磁盘中，并且更新索引的接口

细节处理：

1. 不仅在**Commit** 的时候我们需要**加互斥锁**，在**获取事务序列号**的时候，**同样需要加锁**，防止多个事务同时访问一块数据，造成的数据的不一致性等问题，保证**事务串行化**。
2. 我们在**Commit** 的**加了互斥锁**,在 **appendLogRecord** 中就**不能再加锁**，我们需要修改 appendLogRecord 的逻辑





## 修改数据库的启动时加载索引的逻辑

因为我们实现了批量化提交之后，在我们之前的逻辑处理中，我们启动数据库的时候，直接拿到 LogRecord，然后加载对应的索引信息，然后保存到索引中，但我们实现**批量事务处理**之后，我们并**不能保证每一个数据都成功提交**，所以我们需要先将这些数据**缓存**起来。如果说遍历到最后的时候，我们读到了**事务完成键**这个标识，说明我们这一批事务中的**全部数据都成功提交**，我们才能**更新到内存索引**中去。

1. 我们需要在加载索引之后，**解析LogRecord的Key**，获取实际的Key和序列号（前面实现了序列号在前，Key在后的逻辑），然后**将实际的索引信息保存到索引中**。

解析真实的Key和序列号的函数代码如下：

```
func parseLogRecordKey(key []byte) ([]byte, uint64) {

  // 将无符号整数反序列化为对应的整数值

  seqNum, n := binary.Uvarint(key) //拿到最后一个seqNum的位置，后面的所有都是实际的Key了

  realKey := key[n:]
  
  // 返回真实的Key和序列号

  return realKey, seqNum
}

```

2. 对于事务操作，我们不能直接更新内存索引，要**等待一整批事务全部完成**之后在**依次提交**

实现代码如下:

```
// 缓存我们事务的数据，等待一整批事务都完成之后再更新索引

  transactionRecords := make(map[uint64][]*data.TransactionRecord) //map[序列号]
```

```
// TransactionRecord 缓存事务类型的相关数据

type TransactionRecord struct {

  Record *LogRecord

  Pos   *LogRecordPos

}
```



## 注意：！！！

1. 因为**程序内部实现**的 Put 和 Delete 我们并**没有**对数据（Key和序列号）进行**编码**，因为他们并**非事务操作**，**不会出现竞态条件**（我们本来也没加锁），因此我们可以为他们**加上一个非事务的类型**，辨别某个操作是否是事务类型的操作。

然后我们也将这些**非事务类型**的操作（ Put 和 Delete等等）进行编码，**保证获得索引信息的逻辑处理不改变**。**但是 !**我们并不传入非零值的序列号，保证这条LogRecord中没有序列号存在，相当于**和之前对于所有信息的处理相同**。

代码如下：

```
// nonTransactionSeqNum 标志是否是事务操作

const nonTransactionSeqNum uint64 = 0
```

```
// 构造 LogRecord 结构体实例

  logRecord := data.LogRecord{

​    Key:  logRecordKeyWithSeqNum(key, nonTransactionSeqNum),

​    Value: value,

​    Type:  data.LogRecordNormal,

  }
```



2. 我们遍历数据的时候还需要**更新对应的SeqNum**,保证每次数据库启动的时候，**从最新的序列号开始添加信息序列号**，以实现序列号的严格递增。

```
//标记最新的序列号，方便我们每一批事务都从最新的序列号开始

​      if seqNum > currentSeqNum {

​        currentSeqNum = seqNum

​      }
```



# Merge：清理垃圾数据





如果我们在merge的过程中，遇到了中途退出或者出现故障，但整个merge操作还没有完成的时候，这时候得到的重写文件其实是无意义的。因此我们需要在Merge重写之后的末尾，添加一个标识这个merge完成的文件，如果有这个标识，则代表这里面全都是重写之后的有效数据，如果没有这个标识的话，需要将器其删除后重新进行merge操作。















## 细节处理

1. 返回时释放锁
2. Merge文件需要**从小到大**排序

```
//将Merge的文件从小到大进行排序，依次Merge

  sort.Slice(mergeFiles, func(i, j int) bool {

​    return mergeFiles[i].FileID < mergeFiles[j].FileID

  })


```

3. 每次打开Bitcask 实例的时候不要都Sync 持久化一次，会显著**降低性能**，等**最后结束后再统一Sync**

   ```
   // 所有文件都重写完之后，才开始持久化操作（对最新的hintFile）
   
     if err := hintFile.Sync(); err != nil {
   
   ​    return err
   
     }
   ```

   

4. 已经确定为**有效数据**，在重写的时候不需要处理**事务序列号**了

   ```
   //和内存中的所有进行比较判断，如果是有效的数据则重写
   
   ​      if logRecordPos != nil && logRecordPos.Fid == dataFile.FileID && logRecordPos.Offset == offset {
   
   ​        //写进临时目录当中
   
   ​        logRecord.Key = logRecordKeyWithSeqNum(realKey, nonTransactionSeqNum)
   
   ​      }
   ```

5. 处理hint文件的时候，我们构造一条LogRecord，但其中只储存**编码后**的**索引信息**和**原始的Key**

   ```
   // WriteHintRecord 创建一条Hint文件的logRecord（储存原文件的 Key 和索引信息）
   
   func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
   
     record := &LogRecord{
   
   ​    Key:  key,
   
   ​    Value: EncodeLogRecordPos(pos), //对应的位置索引
   
     }
   
     // 对这个record进行编码
   
     encRecord, _ := EncodeLogRecord(record)
   
     // 文件写入
   
     return df.Write(encRecord)
   
   }
   ```

   

6. merge完成之后，我们需要**将旧的文件删除**，用新的merge文件替代，**但是** ！我们**最后一个merge文件并没有参与merge操作**，我们不能将这个文件删除，否则会导致**数据重写不完整**的情况。

   

7. 开始**删除**对应的**旧数据文件**时，因为我们遵循文件ID递增的原则，我们只能删除比最新一个**没有完成merge操作的文件**对应**FileID更小**的文件！

   

8. 最后我们需要更改Merge文件的目录作为新的文件目录，我们直接**更改文件名称**即可，**不必真的调换数据**

```
// 将新的数据文件移动过来

  for _, fileName := range mergeFileNames {

​    //更改对应的目录

​    //源目录

​    srcPath := filepath.Join(mergePath, fileName)

​    //目标地址

​    desPath := filepath.Join(db.option.DirPath, fileName)

​    //更改名称即可

​    if err := os.Rename(srcPath, desPath); err != nil {

​      return err

​    }

  }
```

 9.我么需要把**Hint文件**的加载放在**数据库启动**的函数里，并且在从数据文件总加载索引之前，**先从hint文件中加载索引**，加载文件的顺序同样如此。

```
// 首先加载 merge 的数据文件
  if err := db.loadMergeFiles(); err != nil {

​    return nil, err

  }
  // 然后加载对应的数据文件
  if err := db.loadDataFiles(); err != nil {
​    return nil, err
  }

  //首先从 hint文件中加载索引
  if err := db.loadIndexFromHintFiles(); err != nil {
​    return nil, err
  }
  // 然后从数据文件中加载索引
  if err := db.loadIndexFromFiles(); err != nil {

​    return nil, err

  }

```



而且，为了**提升性能**，我们在**hint文件**中加载过的数据和索引，在数据文件中**不再重复加载**，同时也避免重复加载。

具体实现逻辑为:我们之前在最后一个merge文件中添加了**标识，**如果说**数据文件中的 FileID 小于这个标识对应的ID**，那么说明这个文件在hint文件中已经被加载过了，**直接跳过**就行。

