#      使用 bitcask 模型实现兼容 Redis 的 KV 储存项目





# 0. 引言：

本次考核要求兼容**Redis**,实现KV存储。KV存储，简单的来说就是由Key和Value组成的一个储存模式。KV存储在**数据库缓存**、**分布式锁**等领域有着广泛的应用。 其设计思想不算复杂，但想要设计出一款比较高效，性能较好的KV储存器，其中有很多细节并不非常简单😁😁😁。

提到**Redis**,我们都知道它是一款性能非常好,非常受开发者欢迎的KV存储产品。但它也并不是完美的，Redis是一款基于内存的**KV存储器**，它运用了一些持久化数据的策略，但从本质上来说Redis还是面向内存设计的KV存储器，因此Redis的性能不言而喻。**有得必有失**，这种设计方式并不是完美的，即使Redis有一些关于持久化的方式，但内存中的数据是**易失性**的，所以Redis对于数据的持久性支持不够。

所以我就想与Redis设计方式有所不同，在简单的框架下实现一个面向磁盘的、数据**可持久性能好**的一款KV存储器，并且在**保证数据持久化**的同时，性能也完全足够用户使用。

在我查看一些资料，也根据自己之前用C++实现KV存储得经验，目前主流的数据存储模型分为两种：**B+Tree**和**LSM Tree**（“日志结构合并树），这个项目中主要使用**B树**中数据结构的特性。

因为这个项目像实现一个数据**持久化到磁盘性能好**的KV存储器。而**Bitcask**就是可以满足这个需求的数据存储引擎的模型。因此这个项目参考了 Bitcask 论文，并**采用了论文中的整体架构思想和设计理念**。

# 1. 关于Bitcask

## 1.1 设计原理：

Bitcask储存模型可以看作系统的一个目录， 并且这个储存模型限制同一时刻只能有一个进程打开这个目录。目录中有多个文件，同一时刻只有**一个活跃文件**用于写入新的数据。但这个活跃文件**不是无序扩张**的，我们需要根据用户需求为其设定一个**临界值**，达到这个临界值之前，我们需要及时将这个活跃文件保存起来，新建一个**新的活跃文件**，再次进行数据储存，而我们将这个被保存起来的活跃文件称为**旧文件**，储存在数据库里。

上面的设计如图：

![image-20240531155742428](C:\Users\shone\AppData\Roaming\Typora\typora-user-images\image-20240531155742428.png)

## 1.2 具体功能实现概略

### 1.2.1 写入数据

为了保证读写数据的性能，上面的写入设计成了**只能写入当前的活跃文件**里面。这种方法有一个最大的**好处**，就是在写数据或者更改数据的时候，**寻址的时间非常短**，极大程度上**提高了并发量**。



### 1.2.2 删除数据

因为我们为了保证储存的性能，采用了**类似于栈方式**的储存模式。因此我们在删除的时候，我们并没有储存需要删除的数据的地址，因此**直接删除数据不可行**。论文中采用了在文件中添加一条数据，标志某个数据**“被删除”的标识**，并且通过索引可以在**当前活跃文件**或者**旧文件**中找到这个需要被删除的数据（**但实际上不能直接删除**）。我们可以用一种遍历数据的思想，遍历所有带有这种“被删除”标识的数据，将它们的索引都保存起来，然后更新的时候真正删除这些数据，我们将这种方式叫做 **Merge** 操作，后面我们会提到这个操作。



### 1.2.3 模型形象化

上面的储存方式可以形象化为如图:

![image-20240531160828769](C:\Users\shone\AppData\Roaming\Typora\typora-user-images\image-20240531160828769.png)

我们通过不同顺序的排序方式，将这些数据以一种高效有序的方式储存起来，然后写入磁盘文件中。然后我们可以通过**B树**的方式，储存这些数据的 **Key** 值（当然其他数据结构的储存模式也是可行的，官方采用的是**哈希表**的储存模式）。

### 1.2.4 读取数据方式

用户读取诗句的时候，程序内部首先根据 key 从**内存中**找到对应的记录，这个记录存储的是数据在**磁盘中**的位置，然后根据这个位置，找到磁盘上对应的**数据文件**，以及文件中的**具体偏移**，这样就能够获取到完整的数据了。

从上面的读取方式可以看出，这种方式要求我们处理好**内存与磁盘之间的交互关系**。在这个项目中，我们通过 **LogRecordPos** 这个结构体来处理**内存**逻辑，通过 **LogRecord** 这个结构体来处理**磁盘**相关的逻辑

结构体代码如下:

```
// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {

  Fid   uint32 //文件的id 表示数据存放到了哪一个文件中

  Offset int64  //偏移量，表示将该数据存放到了文件中哪个位置

}
```

```
// 写入到数据文件的记录（因为是添加写入，所以可以类似看作日志）
type LogRecord struct {

  Key  []byte

  Value []byte

  Type  LogRecordType //墓碑值，可用于标记删除
}
```

## 1.3 本KV储存项目的特性

1. 采用了 Bitcask储存模型，具体实现逻辑比较严谨，储存功能比较齐全。

   

2. 使用了B树这个索引类型（数据结构），随机访问性能优良，并且方便使用**遍历**操作对数据进行 CRUD 操作。

   

3. 持久化性能好，因为内存中并不直接储存数据，而是通过索引将这些数据关联起来，因此储存数据量也比较大。

   

4. 日志数据和数据文件实际上是一个文件，**方便恢复数据**（这也是为什么叫 **LogRecord**）。



# 2. 内存设计

为了**性能高效**，我们需要使用高效的 **CRUD** 方式，这个项目中我们可以使用**B树**。

查阅资料后，我们可以直接运用现成的B树这个数据结构，在GitHub上有这个项目

 https://github.com/google/btree

## 2.1 操作内存的接口

代码如下：（由于文档是项目写到一半的时候写的，这里实现了多个接口）

我们后续还可以继续**根据用户需求添加接口**并且实现逻辑处理

由于内存处理参考了**B树**，这里不多做叙述。

```
// 定义了一个索引的抽象接口，放入一些数据结构（后续可添加）
type Indexer interface {
  // Put 向索引中存储 key 对应数据的位置

  Put(key []byte, pos *data.LogRecordPos) bool

  // Get 根据 key 取出对应索引的位置信息

  Get(key []byte) *data.LogRecordPos

  // Delete 根据 key 删除对应索引的位置信息

  Delete(key []byte) bool

  // 返回迭代器的

  Iterator(reverse bool) Iterator

  // 返回索引中的数据量（Key值）

  Size() int
}
```



**对于磁盘的设计，比较简单，就是一些标准操作文件的API，这里不做赘述。**



# 3. 对数据进行操作

## 1. 写数据操作

对于写数据，只需要两步：1是**将数据写入磁盘中的数据文件**中，2是更新内存索引。

我们首先封装一个日志记录，我们成为LogRecord，里面存储，key、value、类型（有效数据或者被删除了的数据）。

有了保存数据的数据文件（叫日志文件也行），我们需要根据bitcask论文中的阐述来实现追加写入（append-only）。

具体处理逻辑如下：

1. 我们首先判断当前是否有活跃文件，如果有，那么根据活跃文件的偏移量继续写入，如果没有活跃文件，那说明用户是第一次开启这个存储引擎，我们需要创建一个文件，并将这个文件设置为活跃文件。

2. 如果写着写着这个活跃文件的大小达到了我们设置的阈值，我们就需要切换活跃文件（否则文件过大会带来很多麻烦。），然后把之前的活跃文件设置为旧文件，保证活跃文件的唯一性。
3. 如果用户持久化的配置项开启了，那么我们就执行持久化操作。

写完数据文件后，我们就需要更新内存了：

这里维护了一个日志记录的索引：LogRecordPositon ，代表对应数据文件在内存中的索引，主要储存两部分：**数据文件ID**和该**数据文件的偏移量**。

## 2. 读数据操作

读操作实现逻辑如下：

1. 首先我们拿到用户传入的key去内存中寻找索引，如果没有找到则说明这个key 不存在。

2. 然后我们通过索引信息去寻找对应的文件ID，如果没找到返回错误。
3. 如果找到了对应的文件ID，我们根据数据文件的偏移量读取数据即可。

## 3. 删除数据操作

前面提到了我们**不会在数据文件中真正删除数据**，而是对删除的数据**进行“被删除”标记**。因此为了防止恶意删除导致的数据膨胀，即每次删除都标记一条记录。我们需要要求用户只能删除数据库中存在的、有效的数据。（不能删除已经被删除的数据）。所以用户传入key的时候，我们**先判断这个key是否存在**，如果不存在直接返回。

删除的具体逻辑和读操作比较类似，只是再数据文件中多标记一下而已，然后删除内存中对应的索引信息，不多赘述。

但是，这种标记删除也给我们带来一些麻烦，因为我们是在数据文件偏移量后面加上一个“被删除”标识，而且我们为数据文件设置了一个**阈值**，防止文件无限扩张。我们都知道**在内存中存储数据离不开编码，而编码离不开设置字节长度大小，而我们为一条数据设置的字节长度大小，是我们读取数据的基本单元**。这时候，**如果发生末尾的最后一条数据是“被删除”标记**，那么我们如果还按照我们设置的字节长度为基本单元来读取数据，很可能就读到文件末尾了（**EOF错误**）。

为了避免这种情况，我们需要在读取数据的时候，先判断：我们需要读取的字节大小+目前我们索引处的偏移量，是否大于整个数据文件的大小。如果大于，那我们只读取到文件末尾即可。



# 4. 对于数据文件的处理

## 1. 编码操作

bitcask对于编码操作是这样阐述的：

![image-20240621150827898](C:\Users\shone\AppData\Roaming\Typora\typora-user-images\image-20240621150827898.png)

但是我们需要维护一个Type类型来辨别有效数据和“被删除”数据，因此我们需要在此基础上进行加工：

我们编码为：

**CRC+Type+KeySize+ValueSize+Key+Value**

其中：CRC值保证**在数据被读取时**，**得到的CRC值与存储的CRC值进行比较**。如果两者一致，表明数据在存储和读取过程中没有被篡改，可以**确保数据的完整性。**

其他五种编码类型顾名思义，这里不赘述。

## 2. 解码操作

依次拿出上面这六种数据即可，其中需要根据keysize和valuesize获取到用户存储的key和value。

其中需要把读到的CRC值与实际存储的CRC进行校验，看是否相等，**如果相等，说明数据是完整的**，返回给用户即可。





# 5. bitcask存储引擎对用户提供的接口

为了方便用户进行储存操作或者读取操作，我们可以为用户提供一下几个接口：

## 1. Get(Key) ：						  	通过 Key 获取存储的 value



## 2. Put(Key, Value) ：				 存储 key 和 value

## 3. Delete(Key) ： 	  				 删除一个 key

## 4. List_keys() ： 						 获取全部的 key

## 5. Fold(Fun) : 			  			    遍历所有的数据，执行函数 Fun

## 6. Merge(Directory Name) :   清理无效数据

## 7. Sync() :					 				将所有内核缓冲区的写入持久化到磁盘中

## 8. Close() : 								   关闭数据库





# 6. 关于索引迭代器

我们上面为用户提供的接口中，有的需要遍历所有的数据，这时候我们内部需要定义一个**迭代器**，并且让这个迭代器实现顺序遍历、逆序遍历、跳跃遍历这三个功能，方便用户查找数据。

为了实现不同索引类型：B树或者LSM Tree都可以调用迭代器，我们将迭代器**接口抽象**出来

在遍历的时候，我们定义一个函数，让用户进行选择，默认返回true，当用户选择**返回false的时候，迭代结束**

迭代器的定义如下:

## 1. 程序内部的迭代器

### Key：当前遍历位置的 Key 数据





### Value: 当前遍历位置的 Value 数据 





### Next:跳转到下一个 key





### Seek：根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历



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



# 7. 批量写入操作（事务处理）

我们将储存器交给用户使用的时候，绝大多数时间不止一个用户使用，这就需要我们考虑多个读写操作的事务性，而读操作我们之前已经添加了**读锁**。那我们现在需要把多个用户**一次性写入的数据包装为一整个事务**，保证**ACID**性质，避免用户写完之后获取数据时,出现**脏读**,**只能读一次**,**幻读**等情况。因为，我们的**逻辑核心**就在于**实现批量操作的事务性**。

## 具体逻辑处理

对于多个用户的写入操作，我们首先不直接写入磁盘，因为直接写入磁盘可能会出现多个事务一起进行导致上面提到的**并发问题**。我们首先给所有用户的写入操作加入一个**序列号**，将其看作**事务的先后序列**，根据写入时间顺序，对这个序列号进行**严格递增处理**。并且将这个序列号交给 **LogRecord** 内部来处理，保证用户的**一条 LogRecord 对应一个序列号**。对于每一个批次的事务，我们在 **LogRecord** 的最后用一个事务完成键**标记它是否提交**，如果没有提交（写入数据出现的**异常**），那么在整个序列号中，**这个事务不随着整个序列号批次提交**，我们后续使用 **merge** **清理**这些异常数据。

而对于**数据库启动的时候**，当我们读到了这个事务序列号，不直接更新内存索引信息，而是当我们**读到最后的事务完成标志的时候，我们才将这个事务中的所有信息更新。**否则进行**回滚**，然后重新进行事务操作。

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

**将批量写入的缓存数据全部写入磁盘中，并且更新索引的接口**

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



# 8. Merge：清理垃圾数据

Merge操作的实现不算难，但是如果我们在merge的过程中，遇到了**中途退出或者出现故障**，但整个**merge操作还没有完成**的时候，这时候得到的重写文件其实是无意义的。因此我们需要在Merge重写之后的末尾，**添加一个标识这个merge完成的文件**，如果有这个标识，则代表这里面全都是重写之后的有效数据，如果没有这个标识的话，需要将器其删除后重新进行merge操作。



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

   

6. merge完成之后，我们需要**将旧的文件删除**，用新的merge文件替代，**但是** ！我们的**活跃文件并没有参与merge操作**，因为我们要**保证此时用户仍然可以进行写操作**。我们不能将这个文件删除，否则会导致**数据重写不完整**的情况。所以在重新加载数据文件的时候，**需要加载上merge发生时候的活跃文件以及之后新添加进来的文件**。

   

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

 9.我们需要把**Hint文件**的加载放在**数据库启动**的函数里，并且在从数据文件总加载索引之前，**先从hint文件中加载索引**，加载文件的顺序同样如此。

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

具体实现逻辑为:我们之前在最后一个merge文件中添加了**标识，**如果说**数据文件中的 FileID 小于这个标识对应的ID**，那么说明这个文件在hint文件中已经被加载过了，**直接跳过**就行。而数据文件的FIleID大于这个标识ID的文件，都是Merge过程中新添加进来的文件。



# 9. 基于bitcask优化内存索引

## 1.实现多种索引数据结构

我们之前设计了抽象的内存索引接口，我们可以根据实际情况去实现不同的内存索引类型，一个的内存索引结构只需要实现这个抽象接口就可以接入了。这里我们可以使用 **ART 树**（自适应基数树）。

ART树的主要特点：如果子节点只有一个值，则会和父节点进行合并，**减少空间占用**。

ART树的论文： https://db.in.tum.de/~leis/papers/ART.pdf

采用GitHub上的开源库：  https://github.com/plar/go-adaptive-radix-tree



## 2. bitcask内存索引的优缺点

根据 **bitcask** 的论文我们可以得知：bitcask存储模型最大的特点是所有的索引都**只能在内存中维护**。这样的特性带来了一个很大的**好处**，那就是只需要从内存中就能够直接获取到数据的索引信息，然后**只通过一次磁盘IO操作就可以拿到数据了**，但这种方式同样有**缺陷**。如果我们**内存不够**，那么数据库能存储的key+索引的数据量将**受到限制**。

## 3. 优化内容 

既然内存存不下了，那就只有将索引存储到磁盘当中了。例如我们可以使用**更加节省空间**的**B+树**作为索引。但是这种方式也并非完全解决所有问题，如果**将索引存储到了磁盘**当中，**好处**是可以节省内存空间，突破存储引擎的数据量受内存容量的限制，但是随之而来的缺点也很明显，那就是**读写性能会随之降低**，因为需要从磁盘上获取索引，然后再去磁盘数据文件中获取 value。（**没有最好的解决方案，只有最适合的**）

所以我们选择一种折中的方式，既保存我们内存读取索引的一部分性能，又充分利用**B+树**的特性。

查阅资料，我们可以使用 **boltdb** 这个库，这是一个标准的B+树实现。文档链接： https://crates.io/crates/jammdb 

//引用B+树

注意: B+树内部有对于**并发访问的支持**，不需要加锁

```
import (

  "go.etcd.io/bbolt"

)
```



## 4.对于原子写的影响

我们在WriteBatch的时候， 使用B ＋树索引，那么将**不会从数据文件中加载索引**，直接从磁盘中获取对应索引信息即可。但同时我们也拿不到最新的事务序列号，因为**事务序列号是依次加载数据文件中的数据构建索引时获取到的**（而使用B+树后直接从磁盘中索取索引信息，错过了），这会影响事务原子性。

### 解决方法：

数据库 Close 的时候，**将最新的序列号记录到一个文件**当中，启动的时候，**直接从这个文件中获取**。但是因为一些异常情况（比如调用者**没有 Close )** ，导致并没有将序列号记录到这个文件，可以考虑**禁用 WriteBatch** 的功能。

通过这种灵活的方式来决定是否为用户提供原子写的功能，**既**能解决序列号记录的问题，**又**能够为绝大多数用户提供原子写的功能。



## 5.代码实现的细节

1. **ART**并**没有提供反向存储**的方法，因此如果我们想要reverse=ture(反向遍历的话)，需要将索引**反着存储**

2. 我们默认使用**B树**，但可以根据实际情况调整

```
var DefaultOptions = Options{
  DirPath:       os.TempDir(),

  DataFileSize:    256 * 1024 * 1024, // 256MB

  SyncWrites:     false,

  BytesPerSync:    0,

  IndexType:      BTree,   
  }
```



3. 我们另外实现的两种数据结构: ART树和B+树，**都需要添加特定的迭代器**，保证每种数据结构都有自己的迭代器。其中**，B+树**有对应的迭代器（bboltDB），直接拿来用。

4. 注意：使用B+树的迭代器的时候，封装好的迭代器**直接把Key和Value返回回来**，因此我们需要**先缓存起来**。

   代码如下：

   ```
   3. func (c *bbolt.Cursor) Last() (key []byte, value []byte).
   ```

   ```
   if bpi.reverse {
   ​    //将对应的K和V存储起来
   
   ​    bpi.cursorKey, bpi.cursorValue = bpi.cursor.Prev()
   
     } else {
     
   ​    bpi.cursorKey, bpi.cursorValue = bpi.cursor.Next()
     }
   ```

   

5. 注意: 在B+树中，我们一开始实例化索引迭代器的时候，cursorKey和cursorValue都是空的，我们需要一开始**手动调用一次Rewind方法**，**回到迭代器的起点**。

   

6. 如果我们使用的是**B+树**的类型，我们在打开文件的时候，需要**提前把事务序列号取出来**，因为B+树不会从数据文件中加载索引，所以需要提前保存事务序列号。

   

7. 在B+树中，我们要**判断用户是否有存储事务序列号的文件**，如果有的话那么就直接加载即可，如果没有的话我们需要**禁用WriteBach**这个功能，因为拿不到事务序列号。但针对**新用户**，因为一开始就没有事务序列号文件，**我们不能直接禁用功能**。但我们需要判断所有用户是否是第一次加载数据文件。

   但其实对于绝大多数用户，**只要正常关闭数据库**（调用了Close方法），都可以正常使用WriteBach功能

   

   ```
   //如果是B+树索引类型的话并且事务序列号文件不存在并且不是第一次初始化序列号文件的话，需要禁用 WriteBatch 功能
   
   	if db.option.IndexType == BPlusTree && !db.seqNumFileExists && !db.isNewInitial {
   		panic("write batch is banned because no file exists")
   	}
   ```

   

8. 当我们进行Merge操作的时候，我们**不需要**把事务序列号的文件也**拷贝**到数据文件当中，因为我们旧的存储引擎可能又有新的写入。

   在 batch.go中：

```
//如果遍历到了事务序列号的文件，直接跳过

​    if entry.Name() == data.SeqNumFileName {

​      continue

​    }
```



9. 我们在使用**B+树**的时候，**没有更新offset**，因此我们在打开文件的时候，需要设置当前的offset，即**活跃文件目前的大小**。

```
//如果是B+树类型，打开当前事务序列号的文件，取出事务序列号

  if options.IndexType == BPlusTree {
​    //加载事务序列号
​    if err := db.loadSeqNum(); err != nil {
​      return nil, err
​    }
​    //直接将偏移量设置为文件的大小
​    if db.activeFile != nil {

​      size, err := db.activeFile.IOManager.Size()

​      if err != nil {

​        return nil, err
​      }
​      db.activeFile.Offsetnow = size
​    }
  }
```



# 10. 优化文件的输入输出

## 1. 文件锁

由于我们的存储引擎只允许在单个单个进程中使用，但是**用户可能在一个进程中打开多个存储引擎示例**，因此我们需要添加**文件锁**。当我们的系统调用这个文件锁的时候，其他进程尝试对该文件进行加锁时会被**阻塞**，**直到持有锁的进程释放了锁**。这样就能够确保同一时间只有一个进程能够对文件进行操作，**避免**了多个进程同时对文件进行写操作可能导致的**数据不一致或损坏**。

查阅资料后，我们可以使用这个库提供的锁： [https//github.com/gofrs/flock]( https//github.com/gofrs/flock)

使用文件锁后，我们**在打开数据库的时候判断**，如果不能获取文件锁的话，说明有另一个进程正在使用文件锁，返回错误即可。

而我们也需要在 **Close ** 的时候释放这个文件锁。



## 2. 持久化的灵活配置

我们之前提供了一个 **SyncWrites** 的用户配置选项，控制**是否每条数据都持久化到磁盘**，目前的默认值是 false（不进行持久化）。但这种方式**过于死板**，全部交给程序内部处理，如果数据量足够大的时候，全部一次性都持久化必然会影响存储的速度，但要是全都不持久化又有安全隐患。

所以我们可以为用户配置一个选项，**让用户自行决定多少个字节进行一次持久化**，这样更加**灵活方便**。具体实现也不复杂，当写道一个用户自行设置的阈值之后，我们就自动持久化一次，**下一个写入操作重新计算字节数**。



## 3. 对于启动（重启）的优化

根据bitcask论文可知，当我们重启的时候，系统会**加载所有数据文件并构建索引**。但是当我们存储的**数据量足够大**的时候，这种方式会导致我们启动或者**重启的速度不断变慢。**当我们对项目进行维护完毕之后，用户再次启动，则会等待相当长的时间，因此我们需要进行优化。

我们可是使用**将文件映射到内存**的方式来提高我们的启动速度。可以使用**内存映射**(MMap)，将磁盘中的文件映射到内存中，系统在读取文件的时候，会**将磁盘中的数据加载到内存中**，从而**提高系统的IO速度**

由于我们**只需要读取数据**，所以我们可以直接使用现成的库:  https://pkg.go.dev/golang.org/x/exp/mmap

同时我们为用户提供一个配置项，让用户灵活决定是否需要**在启动数据库的时候**使用这种方式。如果需要，我们程序内部需要按照 **MMap**的方式初始化我们的**IOManager** ，**加载索引**的时候读数据也会使用MMap的方式。但是，一旦**启动完成之后**，我们需要需要将 IOManager 重新**切换为原来的类型**。

　

## 4. 细节处理

1. 我们需要**每次判断**用户是否打开了 **SyncWrites** 即每次写入都需要持久化，**如果用户没打开**，这时我们才需要判断用户写入的数据是否达到了需要持久化的**阈值**。

2. 我们只使用mmap来读取数据，对于所有其他情况，我们都默认使用golang标准的文件操作封装。

   ```
    type FileID struct {
        fd *os.File //系统文件描述码
      }
   ```

3. 当我们启动完成之后，需要将数据文件的 IO　类型全部设置为默认的类型（这个默认也可以根据需要调整）

```
func (db *DB) resetIOType() error {
  //当前活跃文件为空直接返回
  if db.activeFile == nil {
​    return nil
  }
  //设置当前活跃文件
  if err := db.activeFile.SetIOManager(db.option.DirPath, fio.StandardFIO); err != nil {
​    return err
  }
  //遍历设置旧的数据文件
  for _, dataFile := range db.oldFiles {
​    if err := dataFile.SetIOManager(db.option.DirPath, fio.StandardFIO); err != nil {
​      return err
​    }
  }
  return nil
}
```



# 11. 优化Merge操作

## 1. 解决低效遍历问题

如果说在我们的储存器中，用户存储的**数据量已经很大**了，但是用户删除的数据量（**无效数据量）相对较少**。在这种情况下，如果我们让用户指定一段时间后进行一次merge操作，那么很可能用户的数据文件中**根本没有或者只有很少的无效数据**需要被Merge清理掉。但我们还是对整个数据文件进行Merge遍历操作，就让**效率非常低**。

因此，我们需要为用户提供一个配置项，让用户来决定，当数据文件中的**无效数据达到多少数据量的时候**，再来进行**Merge清除**操作。这样就可以避免程序做很多无用功。

我们可以在内存索引中维护一个值，记录每条数据**在磁盘上的大小**，Delete数据的时候，可以得到旧的值，这个旧的值就是磁盘上**失效的数据。**Put 存数据的时候，如果判断到有旧的数据存在，那么也同样**累加**这个值，这样我们就能够从Put/Delete 数据的流程中，得到**失效数据的累计值。**

### 细节处理：

1. 我们在删除的时候，向数据文件中追加写入了一条标记删除的值（**墓碑值**），而这条数据在**merge之后**其实是**没有意义**的，因此我们**也应该将它删除**。

```
// 写入数据文件中
  pos, err := db.appendLogRecordWithLock(logRecord)
  if err != nil {
​    return nil
  }
  //将删除这个标记也标记为删除
  db.reclaimSize += int64(pos.Size)
```

2. 如果目前的**无效数据量**，还**没达到**用户所设置的**阈值**，那么直接返回错误，不进行merge操作

   在 merge.go 中

   ```
   //查看当前无效数据是否达到用户设置的merge ratio 阈值
     totalSize, err := utils.DirSize(db.option.DirPath)
     
     if err != nil {
   ​    //注意，要解锁
   ​    db.rwmu.Unlock()
     }
     //算出比例,如果还没达到用户设置的阈值就直接返回
     if float32(db.reclaimSize)/float32(totalSize) < db.option.DataFileMergeRatio {
   ​    db.rwmu.Unlock()
   ​    return ErrUnderMergeRatio
     }
    
   ```
   

## 2. 防止Merge导致的内存溢出

 我们在Merge操作的时候，会将目前有效的数据文件写入一个临时目录。如果我们的无效数据很少，那么此时磁盘上的数据就无限接近于原来磁盘数据空间的两倍，有可能会导致磁盘空间不够的情况。

为了避免这种情况，我们应该在Merge之前就**提前算出**，在我们将现有数据放入临时目录之后，**占用磁盘空间的总和**，如果不**超过磁盘空间我们才进行Merge操作**，否则返回错误。我们直接采用GitHub上别人获取当前目录下的磁盘剩余可用空间大小的代码。

在 merge.go 中：

```
//查看剩余空间容量是否可以装下Merge后的数据量
  availableDiskSize, err := utils.AvailableDiskSize()

  if err != nil {

​    db.rwmu.Unlock()

​    return err
  }
  //如果超过了磁盘的容量直接返回错误
  if uint64(totalSize-db.reclaimSize) >= availableDiskSize {

​    db.rwmu.Unlock()

​    return ErrNotEnoughSpaceToMerge
  }
```



# 12. 数据备份处理

对于当前我们实现的存储器，用户存储的数据文件都放在了同一个目录当中，如果这个目录中的**数据出现损坏**，或者目录所在磁盘出现了故障，那么都会导致整个存储引擎实例不可用，造成**数据丢失**。

根据bitcask的论文，我们只需要**将数据目录拷贝到其他的位置**，这样就算原有的目录损坏了，拷贝的目录中仍然存有备份的数据，可以直接在这个新的目录中启动bitcask，**保证数据不丢失**。

我们可以提供一个方法，这个方法接受一个目录路径，我们将数据文件复制一份到这个目录路径下就可以了。

**注意：！！！** 我们在拷贝数据目录的时候，需要排除这个文件对应的文件锁。当再次打开存储器的时候，再重新再这个数据目录重申请一个文件锁。



# 13. 与HTTP协议集成

为了用户更加方便的读写数据，我们可以将存储器**与HTTP进行集成**，这样也方便了用户与其他系统进行**交互**。通过使用HTTP协议的

 请求方式，可以快速地从存储引擎中**增、删、查**数据。

所以我们可以运用一种常见的golang的web框架来实现一个HTTP的接口。但为了尽量**减少耦合度**，这个项目就使用**golang自带的HTTP框架**来完成这个接口。



# 14. 兼容部分 Redis 数据结构

为了兼容Redis中的数据结构，我们需要将这些数据结构与我们实现的KV存储器的结构关联起来。我们可以对这些数据结构进行编码解码的操作，方便我们对它们进行存储。

主要兼容了下面五个数据结构：

```
// 设置redis数据结构类型常量
const (
  String RedisDataType = iota

  Hash

  Set

  List

  ZSet
)
```

**这里对其中三个例子进行解释，其他数据类型实现方法都比较类似:**

## String

既然需要兼容Redis，我们就不能简单的只实现KV之间的对应关系，我们还需要兼容这个数据类型满足的一些命令行。比如: set key1 value1 ， get key1 , type key1 等命令。但是，我们究其根本还是要**回归到对于K和V的处理**。因此我们需要先对用户需要获取的内容和我们程序内部需要处理的内容进行**封装**。因此，我们需要将KV中的V**编码封装**为: **value = 数据类型 +过期时间+用户存储的原始Value**

用这种编码方式封装我们需要的的集合 V 方便我们**处理过期的数据** (Redis对于数据的持久性一般，因此会进行清理数据的操作)，同时可以**兼容 Type** 这个命令操作，而且还可以拿到用户存储的真实value。（**后面几个数据类型的处理在逻辑上与String类型相似，即：编码封装，解码获取**）

当我们**读取数据时**，我们就需要把以上面这种编码方式编码的一条信息进行**解码**，分别拿出并**进行逻辑处理**

代码如下:

```
// 解码
	dataType := encvalue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

var index = 1
//拿到过期时间
expireTime, n := binary.Varint(encvalue[index:])
index += n

//如果过期了就直接返回
if expireTime > 0 && expireTime <= time.Now().UnixNano() {
	return nil, nil
}

// 拿到原始数据并返回
return encvalue[index:], nil
```

## Set

Set 和 ZSet 处理逻辑差不多，下面再 ZSet 处一起说

## Hash

为了兼容Redis中其他命令，比如:查看过期时间，查看size等命令，我们同样需要将 V 进行编码处理，以便于兼容。因此**编码公式如下 :**

**Value =  数据类型 + 过期时间 + 目前数据的版本号 + 目前数据的SIze**

秉承着 bitcask 模型的思想，我们在删除数据的时候，**不能直接通过遍历的方式找到需要删除的数据**，然后删除，因为这样的删除方式太过低效。我们封装的 **Delete** 函数使用的是添加墓碑值的方法，但这里显然没必要为了删除而增加耦合度。因此我们选择另一种标记删除的方法，即通过标记版本号来标记数据，**保证我们的有效数据所持有的版本号永远是最新的**（版本号保持严格递增）。对于那些被标记删除的数据，其持有的版本号显然小于最新版本号，我们**在下次Merge的时候清除这些持有旧版本号的数据即可**。这种方式还避免了直接删除key导致其他数据无法访问的情况。

同理，我们如果使用遍历的方式获取目前数据的Size，这样也非常低效，因此我们维护一个记录目前数据Size的变量，每次set就+1，每次删除就-1。

以这条命令行为例：**HSET mykel myfield myvalue**

因为有了版本号，所以我们获取真实数据的时候，需要同时**拿着Key和Version还有field来获取用户存储的真实数据**，以保证我们**不会访问到已经被删除了的数据。**同时field的存在让我们可以精确定位某个字段，然后取出用户实际存储的数据

具体编码处理如下:

```
// hashInternalKey Hash类型需要编码成真正Key的结构体
type hashInternalKey struct {
	key     []byte
	field   []byte
	version int64
}

// 编码 Hash 真正的Key
func (h *hashInternalKey) encode() []byte {
	// 计算字节数
	buf := make([]byte, len(h.key)+len(h.field)+8) //version最多占八位

// key
var index = 0
copy(buf[index:index+len(h.key)], h.key)
index += len(h.key)

// version
binary.LittleEndian.PutUint64(buf[index:index+8], uint64(h.version)) //确定字节大小的类型就用小端序
index += 8                                                           //最多 8 字节

// field
copy(buf[index:], h.field)

return buf
}
```

我们可以把 key+field+version **看作**我们索引value的真正的Key（不是用户存储的key，这里只是看作），解码后拿出，然后索引真实的value即可。

## List

对于List，我们可以将这个数据结构看作一个队列，而为了兼容命令行，即在队列头部和尾部进行增、减操作。**我们需要维护头部和尾部这两个变量，以便于我们直接在头部或者尾部进行操作**，而其他的兼容部分和上面两个数据结构基本相同，这里重点阐述对于头部和尾部的处理。

我们可以将一条List看作一条 LogRecord,而这条LogRecord需要镶嵌在一条更长的数据之中。因此，**为了头部尾部的插入不会超出索引**，在没有数据插入之前，我们将头部和尾部的指针，都放在这条更长数据的中间（ binary.MaxVarintLen64 / 2 ）。然后每次从头部插入，头部索引就-1，每次从尾部插入，尾部索引就+1。以这种方式，**随着这条List数据越来越多，这条 LogRecord 也越来越长，中间部分即为用户存储的数据**。

那么可能有人会问，万一这条更长的数据被填满了怎么办呢，接着填入的数据不是会超出索引范围吗？为了避免这种情况发生，我们采用 binary.MaxVarintLen64 来作为这条更长数据的长度，用不完，根本用不完！

对于这种不同的处理方式，我们也在编码元数据的时候加上对于的 if 语句 判定，**如果是 List 类型的话，就编码上对应的数据即可。**

```
// encodeMetaData 编码元数据，转化为字节数组类型
func (md *metadata) encodeMetaData() []byte {
	var size = maxMetadataSize

// 如果发现是 List 类型，多加上 List 专属的字段
if md.dataType == List {
	size += ListMetadataSize
}

buf := make([]byte, size)

//获取数据类型
buf[0] = md.dataType
var index = 1

//获取过期时间、版本号和数据Size，并调整索引
index += binary.PutVarint(buf[index:], md.expireTime)
index += binary.PutVarint(buf[index:], md.version)
index += binary.PutVarint(buf[index:], int64(md.size))

//如果是 List 类型还要取出 head 和 tail
if md.dataType == List {
	index += binary.PutUvarint(buf[index:], md.head)
	index += binary.PutUvarint(buf[index:], md.tail)
}

//返回编码后数据
return buf[:index]

}
```

## ZSet

对于元数据部分，Set 和 ZSet 是一样的，通过key 索引V，并且将V进行封装：

 **Value =  数据类型 + 过期时间 + 目前数据的版本号 + 目前数据的SIze**

首先，对于ZSet,我们获取对于Key的Score，因此我们需要维护这个Score。为了保证用户存储的value不重复，我们通过Key和member还有version(主要负责处理删除逻辑)来维护这个 Score。**member则主要负责判断用户是否重复传入多个相同的value，保证不重复**。

其次，我们还需要通过Score来保证按照Score进行排序。**因此我们还需要存储一部分数据来保证顺序获取value。**这一部分数据我们通过 Key+Member+Version+Score+MemberSize 来存储。其中Key、Member是为了索引，Version 用于删除数据、MemberSize 用于获取

用户每次存储了多少个value，方便作为命令行的响应。而最重要的**Score 则用于顺序获取Value。**

这两部分的编码分别如下:

第一部分：

```
func (z *zsetInternalKey) encodeMember() []byte {
	buf := make([]byte, len(z.key)+len(z.member)+8)

// 编码 key
var index = 0
copy(buf[index:index+len(z.key)], z.key)
index += len(z.key)

// 编码 version
binary.LittleEndian.PutUint64(buf[index:index+8], uint64(z.version))
index += 8

//编码 member
copy(buf[index:index+len(z.member)], z.member)

return buf
}
```

第二部分：

```
func (z *zsetInternalKey) encodeScore() []byte {
	scoreBuf := utils.Float64ToBytes(z.score)

buf := make([]byte, len(z.key)+len(z.member)+len(scoreBuf)+8+4)

// 编码 key
var index = 0
copy(buf[index:index+len(z.key)], z.key)
index += len(z.key)

// 编码 version
binary.LittleEndian.PutUint64(buf[index:index+8], uint64(z.version))
index += 8

// 编码 score
copy(buf[index:index+len(scoreBuf)], scoreBuf)
index += len(scoreBuf)

//编码 member
copy(buf[index:index+len(z.member)], z.member)
index += len(z.member)

// 加上 member 的长度 member 的 size ,方便我们获取 member
binary.LittleEndian.PutUint32(buf[index:], uint32(len(z.member)))

return buf
}
```

# 15. 系统监控

该项目的系统监控主要采用**使用 SMTP 发送报警信号到QQ邮箱**，即发送警报信息给指定的维护人员。

如果说这个KV存储器报警之后，维护人员可以第一时间获取消息。如果这个存储器挂掉之后，开发人员可以第一时间重启数据库，利用redis的 RDB 和 aof 持久化机制来写入磁盘，快速的恢复储存的数据。

## 监控内容

主要获取这四项数据: 

最新的数据库CPU 占比
存储引擎的内存总量
存储引擎目前使用的内存量
存储引擎当前空闲的内存量

主要通过持续监控系统指标，看这些指标**是否超出阈值**（CPU占用超出阈值对于存储器性能有较大影响，内存占用超出阈值说明内部Merge操作可能存在问题或者遭到了洪水攻击）。每隔五秒进行一次监测。

代码如下: （其中： interval := 5 * time.Second）

```
for {
		metrics := collectSystemMetrics()

​	// 存储监控数据到数据库
​	if err := storeMetricsToDatabase(metrics); err != nil {
​		log.Println("Error storing metrics:", err)
​	}

​	// 检查告警
​	checkAlert(metrics, threshold)

​	// 休眠一段时间
​	time.Sleep(interval)
}
```

## 监控数据存储

将监控获得的 KV 存储器的各项数据指标存储在 MySQL 数据库中，便于维护人员定时查看数据，尽量避免报警情况的出现。

代码如下:

```
func storeMetricsToDatabase(metrics SystemMetrics) error {
	// 连接到 MySQL 数据库
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

// 插入数据
_, err = db.Exec("INSERT INTO metrics (cpu_percent, memory_total, memory_used, memory_free) VALUES (?, ?, ?, ?)",
	metrics.CPUPercent, metrics.MemoryTotal, metrics.MemoryUsed, metrics.MemoryFree)
if err != nil {
	return err
}

return nil

}
```

**注意 ！！！**

1. **由于维护人员的接受邮箱基本上是固定的，发送者的邮箱也基本上是固定的，这里直接写为常量，如有变动可以直接更改。**

2. **由于我们设定的CPU的阈值为达到存储器的百分之八十，很难达到，内存阈值也一样，因此没有添加对应的单元测试示例。**



# 16. 基准测试

基准测试结果如下: 

```
Running tool: /root/go/bin/go test -benchmem -run=^$ -bench ^Benchmark_Get$ bitcask.go/benchmark

goos: linux

goarch: amd64

pkg: bitcask.go/benchmark

cpu: 13th Gen Intel(R) Core(TM) i5-13500H

Benchmark_Get-4    1306245      1123 ns/op     135 B/op      4 allocs/op

PASS

ok    bitcask.go/benchmark   3.829s
```

由数据可以看到，KV存储器每秒**可以至少进行百万数量级的操作**，每次操作用时也非常短。由此可见，该 KV 存储器的性能比较优秀，实现了我们一开始的设计理念: **完全足够用户使用的性能，并且持久化性能优秀**。

基准测试实在虚拟机上进行的，并且我的虚拟机分配的资源比较少：

![image-20240614142839662](C:\Users\shone\AppData\Roaming\Typora\typora-user-images\image-20240614142839662.png)

**因此在性能较好的机器上运行该KV存储器，性能会更好。**



# 17. 写项目中遇到的较大的困难/BUG

1. 当我在初始化B+树的时候，我们需要传入是否需要持久化，而**B+树这个包内部为我们实现了这个需求**，但是！！！NoSync标识对是否需要持久化取反，我一开始以为只需要让NoSync这个变量与我们传入的是否取反保持一致即可，但是测试的时候怎么也通不过，卡了很久。然后去官方看源代码才发现封装的是否持久化加了一个**取反**操作！！！😢😢😢

```
// NewBPlusTree 初始化B+树

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {

  //传入配置项，根据实际情况调用

  options := bbolt.DefaultOptions

  options.NoSync = !syncWrites //取反操作，保持一致
```

2. 启动优化。我在看一篇关于开机启动的论文的时候，了解到当**数据库启动**的时候，短时间**数据量的加载是很大**的，通常开发者都会**对启动数据库做一些优化**。通常采用的是**MMap**。但是我不太了解MMap的应用场景，因此看了几篇论文，问了学长几个问题，最终还是决定使用mmap对数据库的启动做一些优化。当我想提供一个**配置项**，让用户来决定使用**golang标准的IO模式**还是使用**MMap的IO模式**的时候，我发现如果重新写MMap的配置项，几乎要全部重新架构一遍，但我只对数据库启动做些优化。因此对于MMap中**除了Read之外的方法**，我都没有使用，这样也**避免了重写很多不必要的代码**。

3. 对于在兼容Redis数据类型和命令行的时候，当我们**删除数据时**，**实际上是添加一个标识符来表示某个数据已经被删除了**，保证我们永远访问的是有效的数据，而被删除的数据不可能被访问。但是**对于清理被删除的数据**，一开始我认为需要再添加一个Merge操作来定期清除，或者遍历所有的 field ，然后删除所有被标记为删除的数据（因为field相比Key来说数据量小很多）。但其实遍历还是很低效的方法。之后我发现其实可以直接将这些数据存入数据文件，**通过之前实现的Merge操作来清除这些数据**。

4. 关于监控死锁问题。一开始是想为监控器添加监控死锁的功能，但是对于死锁的监控位置不是很清楚（单体应用，我在基本上该上锁的地方已经上锁了），如果说盲目再每个加锁的地方都上锁的话，会影响监控器占用CPU比值和其他监控功能，因此最后就只写了文件锁，防止一个用户手动打开多个存储引擎实例。

# 该项目后续可以扩展的地方

1. 可以使用**更多种索引的数据类型**进行索引

2. 只设计了一个索引的结构，我们**只有**这个结构中的一把锁。那么当读写操作同时进行的时候，用户量少影响不大，但是在高并发的背景下，这把锁的加锁解锁操作会很大程度上**限制我们的性能**。我们可以在学习高并发对锁的处理后**，降低锁的粒度**，实现为选择**不同索引数据类型**的客户提供同一把锁，或者为每个用户提供一把锁。（只是本人的猜想，不知道可不可行）。

3. 为管理人员提供权限服务，比如: 添加新功能后，测试员没有权限不能读和写存储器中的数据文件。

4. 兼容更多的 Redis 数据结构和更多的 Redis 命令行。



# 考核演示代码部分:

## 1. HTTP展示代码

发送请求，设置 KV 值

curl -X POST localhost:8080/bitcask/put -d '{"name1":"shone","name2":"Aaron"}'

获取 V

curl "localhost:8080/bitcask/get?key=name1"



## 2. 兼容redis协议展示代码

首先进入cmd目录：     ./cmd

redis-cli -p 6380(端口号)

1. set k1 100

2. get k1

3. set k1 200

4. get k1 (更新这个Key的值，以最新的为准)



获取不存在的数据

5. get k2



其他命令行

// hset key field value

1. hset key1 field1 100

多输入一个参数，报错

2. hset key1 field1 100 200

获取对应数据

3. hset myset a 999

4. hget myset a

修改数据后再获取:

5.  hset myset a 888

6. hget myset a



// ZADD key score1 member1 [score2 member2 ...]

7. zadd myset 100 a
8. zscore myset a

