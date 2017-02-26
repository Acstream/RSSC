package class3

import breeze.linalg.{DenseMatrix, eigSym}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonAST.{JField, JInt, JObject, JString}
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yuanyue on 2017/2/16.
  */
object RealtimeStockSpectralClustering
{
  /**
    * SPARK应用配置属性
    */
  val APPNAME:String="StockSpectralClustering"//应用名称
  val MASTERURL:String="spark://10.175.201.81:7077"//MASTER服务器URL地址(注意spark://开头和:7077端口结尾)
  val PROPERTY_1_TYPE:String="spark.executor.memory"//配置属性1——执行内存
  val PROPERTY_1_VALUE:String="16g"//配置属性1值——执行内存16G
  val JARSPATH:List[String]=List("/Users/yuanyue/IdeaProjects/class3/out/artifacts/class3_jar/class3.jar")//程序生成Jar包位置(每次修改记得重新rebuild)

  val ORIGINALFILEPATH:String="/user/spark/yy/data1.txt"//存放股票数据.txt文件的HDFS路径
  val CLUSTERNUM:Int=6//聚类个数
  val STANDARD_NUM_OF_EACH_GROUP:Int=100//标准数据每组个数
  val SIGMA_INDEX:Int=7

  val TIMELEAP:Int=60//时间间隔
  val DATA_SERVER_IP:String="222.20.138.2"
  val DATA_SERVER_PORT:Int=30001

  /**
    * 读取数据(从HDFS中以.txt文件形式读取)
    * @param hadoopFilePath HDFS中.txt文件的路径
    *                       e.g.txt文件内的形式
    *                       1,2.01,2.01,2.02,2.02,...,2.11
    *                       2,10.13,10.14,10.15,...,10.25
    *                       3,14.64,14.64,14.65,...,15.02
    *                       .............................(前600条为已经分好类的标准数据,1~100为普通,101~200为周期,201~300为向上趋势,301~400为向下趋势,401~500为向上平移,501~601为向下平移)
    *                       开头第一个数据为该支股票的编号,后面的一串数字为每隔1分钟该股票的价格(即共有480个数据)
    * @param sc Spark环境变量
    * @return RDD形式的原始数据——RDD[String]
    */
  def readData_HDFS(hadoopFilePath:String,sc:SparkContext): RDD[String] =
  {
    sc.textFile(hadoopFilePath)
  }

  /**
    * 对从HDFS读取的原始数据进行预处理
    * @param originalData 原始数据——RDD[String]
    * @return RDD形式处理后的原始数据——RDD[(String, List[(Double, Long)])]
    *                                    股票编号       股票价格,计数器(该支每新增一个价格计数器加1)
    */
  def pretreatOriginalData(originalData:RDD[String]): RDD[(String, List[(Double, Long)])] =
  {
    val pretreatedData:RDD[(String, List[(Double, Long)])]=originalData.map(//map——tranformation操作,对原始数据进行处理
      line =>
      {//每行数据进行如下处理
        val lineArray:Array[String]=line.split(",")//分割逗号使改行数据变成一个数组
        var priceList:List[(Double,Long)]=List[(Double,Long)]()//定义存储价格的List
        for(i <- 1 to lineArray.length-1)//从第2个数字开始遍历该行数据的价格并存入List
        {
          priceList=priceList:+((lineArray(i).toDouble),i.toLong)//i为计数单位
        }
        (lineArray(0),priceList)//最终返回RDD[(String, List[(Double, Long)])]形式的数据
      }).filter(element => element._1.isEmpty()==false)//防止处理数据时出现空行(只有回车)的情况
    pretreatedData//返回预处理后的数据
  }

  /**
    * 计算两支股票的DTW距离
    * @param twoStocksPriceBuffer 两支股票价格构成的2*n(n为股票价格的个数，这里两支股票的价格都有480个故为2*480)的二维ArrayBuffer数组
    * @param startPointValue 起始点的数值
    * @param startPointUpValue 起始点上方的数值
    * @param startPointRightValue 起始点右方的数值
    * @return Array[ Array[ Double] ]形式的计算后数据——股票两两之间的DTW距离
    */
  def calculateDTW(twoStocksPriceBuffer:ArrayBuffer[ArrayBuffer[Double]],startPointValue:Double,startPointUpValue:Double,startPointRightValue:Double):Array[Array[Double]]=
  {
    //定义并初始化绝对值差距数组,用于存储两支股票两两间对应价格的(绝对)距离
    val absDifferenceArray:Array[Array[Double]]=new Array(twoStocksPriceBuffer(0).length)
    for(i <- 0 to absDifferenceArray.length-1)
    {
      absDifferenceArray(i)=new Array(twoStocksPriceBuffer(1).length)
    }

    //计算两支股票两两间对应价格的(绝对)距离
    for(i <- 0 to twoStocksPriceBuffer(0).length-1)
    {
      for(j <- 0 to twoStocksPriceBuffer(1).length-1)
      {
        absDifferenceArray(i)(j)=Math.abs(BigDecimal(twoStocksPriceBuffer(0)(i)-twoStocksPriceBuffer(1)(j)).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble)
      }
    }

    //定义并初始化两支股票的DTW距离数组
    val distanceArray:Array[Array[Double]]=new Array(absDifferenceArray.length+1)
    for(i <- 0 to distanceArray.length-1)
    {
      distanceArray(i)=new Array(absDifferenceArray(0).length+1)
    }
    distanceArray(0)(0)=startPointValue
    for(i <- 1 to distanceArray.length-1)
    {
      distanceArray(i)(0)=startPointUpValue
      for(j <- 1 to distanceArray(i).length-1)
      {
        distanceArray(0)(j)=startPointRightValue
      }
    }

    //动态规划计算DTW距离
    for(i <- 1 to distanceArray.length-1)
    {
      for(j <- 1 to distanceArray(i).length-1)
      {
        //状态转移方程
        distanceArray(i)(j) = BigDecimal((Math.min(Math.min(distanceArray(i)(j - 1), distanceArray(i - 1)(j)), distanceArray(i - 1)(j - 1))) + absDifferenceArray(i - 1)(j - 1)).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble
      }
    }
    distanceArray//返回DTW距离矩阵
  }

  /**
    * SDTW计算流程
    * @param data 两组股票之间的按窗口分隔的组合，最外层的数组是时间窗口的分隔，内层二维数组是两支股票在这个时间窗口内价格序列的的组合
    * @return 返回最终的SDTW计算出的DTW距离
    */
  def SDTW(data:ArrayBuffer[ArrayBuffer[ArrayBuffer[Double]]]):Double=
  {
    val calculateData:ArrayBuffer[ArrayBuffer[ArrayBuffer[Double]]]=data
    var tempArray:Array[Array[Double]]=null//临时数组存储某个时间窗口内数据的计算结果
    for(i <- 0 to calculateData.length-1)//从0到有时间窗口的数量
    {
      if(i==0)//当i为0时，即第一个时间窗口时，即开始时按照DTW计算
      {
        tempArray=calculateDTW(calculateData(i),0,Double.MaxValue,Double.MaxValue)
      }
      else
      {
        val tempStartPoint=tempArray(tempArray.length-2)(tempArray.length-2)//起始点
      val tempStartRightPoint=tempArray(tempArray.length-2)(tempArray.length-1)//起始点右边的点
      val tempStartUpPoint=tempArray(tempArray.length-1)(tempArray.length-2)//起始点上边的点
        tempArray=calculateDTW(calculateData(i),tempStartPoint,tempStartUpPoint,tempStartRightPoint)//按照DTW计算每个时间窗口
      }
    }
    tempArray(tempArray.length-1)(tempArray(0).length -1)//返回最终的计算结果
  }


  /**
    * 计算每两支股票之间的DTW距离
    * @param pretreatedData 预处理后的数据
    * @return Array形式的数据——Array[(String, List[(String, Double)], Array[Double])]
    *                               股票A编号      股票B编号 A与B之间DTW距离 距离的数组(用于求出sigma值)
    */
  def getDistanceAndSigmaOfEveryTwoStocks(pretreatedData:RDD[(String, List[(Double, Long)])]): (Array[Array[Double]],Array[Double]) =
  {
    //预处理后的数据做笛卡尔积得到股票之间两两的组合
    val result=pretreatedData.cartesian(pretreatedData).map(element =>
    {
      val finalResultArrayBuffer:ArrayBuffer[ArrayBuffer[ArrayBuffer[Double]]]=new ArrayBuffer[ArrayBuffer[ArrayBuffer[Double]]]()
      var arrayBuffer1:ArrayBuffer[Double]=new ArrayBuffer[Double]()
      var arrayBuffer2:ArrayBuffer[Double]=new ArrayBuffer[Double]()
      var start:Int=0
      for(i <- 0 to element._1._2.length-1)
      {
        if(i==element._1._2.length-1)
        {
          arrayBuffer1+=element._1._2(i)._1
          arrayBuffer2+=element._2._2(i)._1
          val timeLeapArrayBuffer:ArrayBuffer[ArrayBuffer[Double]]=new ArrayBuffer[ArrayBuffer[Double]]()
          timeLeapArrayBuffer+=arrayBuffer1
          timeLeapArrayBuffer+=arrayBuffer2
          finalResultArrayBuffer+=timeLeapArrayBuffer
        }
        else if(i-start>=5)
        {
          start=i
          arrayBuffer1+=element._1._2(i)._1
          arrayBuffer2+=element._2._2(i)._1
          val timeLeapArrayBuffer:ArrayBuffer[ArrayBuffer[Double]]=new ArrayBuffer[ArrayBuffer[Double]]()
          timeLeapArrayBuffer+=arrayBuffer1
          timeLeapArrayBuffer+=arrayBuffer2
          finalResultArrayBuffer+=timeLeapArrayBuffer
          arrayBuffer1=new ArrayBuffer[Double]()
          arrayBuffer2=new ArrayBuffer[Double]()
        }
        else
        {
          arrayBuffer1+=element._1._2(i)._1
          arrayBuffer2+=element._2._2(i)._1
          val timeLeapArrayBuffer:ArrayBuffer[ArrayBuffer[Double]]=new ArrayBuffer[ArrayBuffer[Double]]()
          timeLeapArrayBuffer+=arrayBuffer1
          timeLeapArrayBuffer+=arrayBuffer2
          finalResultArrayBuffer+=timeLeapArrayBuffer
        }
      }
      ((element._1._1+"-"+element._2._1).toString(),finalResultArrayBuffer)
    }).map(element => (element._1,SDTW(element._2))).map(element => {
      (element ._1.split("-")(0),List((element._1.split("-")(1),element._2)))//结果形式("股票A编号",List("股票B编号",两支股票之间DTW距离))
    }).reduceByKey((element1_2,element2_2) => {
      element1_2++element2_2//reduce拼接两个List
    }).map(element => {//每组数据增加一个ArrayBuffer记录，ArrayBuffer存放股票两两之间DTW距离（sort后ArrayBuffer）
      var arrayBuffer:ArrayBuffer[Double]=new ArrayBuffer[Double]()
      for(i <- 0 to element._2.length-1)
      {
        arrayBuffer+=element._2(i)._2
      }
      val afterSort:Array[Double]=arrayBuffer.toArray
      java.util.Arrays.sort(afterSort)
      (element._1,element._2,afterSort)
    }).collect()//最后action操作

    //定义并初始化距离数组
    val distance:Array[Array[Double]]=new Array(result.length)
    for(i <- 0 to distance.length-1)
    {
      distance(i)=new Array[Double](distance.length)
    }
    //求出距离和sigma数组
    val sigma:Array[Double]=new Array(result.length)
    for(i <- 0 to result.length-1)
    {
      val x:Int=result(i)._1.toInt-1
      sigma(x)=result(i)._3(SIGMA_INDEX)
      for(j <- 0 to result(i)._2.length-1)
      {
        val y:Int=result(i)._2(j)._1.toInt-1
        distance(x)(y)=result(i)._2(j)._2.toDouble
      }
    }
    (distance,sigma)//最终返回结果
  }

  /**
    * 计算相似度矩阵
    * @param dtwDistance dtw距离数组
    * @param sigma sigma数组
    * @return 相似度数组ArrayBuffer[ ArrayBuffer[ Double ] ]
    */
  def calculateSimilarity(dtwDistance:Array[Array[Double]],sigma:Array[Double]): ArrayBuffer[ArrayBuffer[Double]] =
  {
    val similarity:ArrayBuffer[ArrayBuffer[Double]]=new ArrayBuffer[ArrayBuffer[Double]]()
    for(i <- 0 to dtwDistance.length-1)
    {
      val temp:ArrayBuffer[Double]=new ArrayBuffer[Double]()
      for(j <- 0 to dtwDistance(i).length-1)
      {
        if(i==j)//相等相似度为0
        {
          val calculateResult=0.0
          temp += calculateResult
        }
        else
        {
          val calculateResult = Math.pow(Math.E, dtwDistance(i)(j) * (-1) / (2 * sigma(i) * sigma(j)))//相似度矩阵归一化公式
          temp += calculateResult
        }
      }
      similarity+=temp
    }
    similarity
  }

  /**
    * 计算获取拉普拉斯矩阵
    * @param similarity 相似度矩阵
    * @return 拉普拉斯矩阵——Array [ Array [ Double ] ]
    */
  def getLaplacianMatrix(similarity:ArrayBuffer[ArrayBuffer[Double]]): Array[Array[Double]] =
  {
    //定义相似度矩阵并将相似度矩阵转换为Array形式
    val similarityArray:Array[Array[Double]]=new Array(similarity.length)
    for(i <- 0 to similarity.length-1)
    {
      similarityArray(i)=new Array[Double](similarity(i).length)
      for(j <- 0 to similarity(i).length-1)
      {
        similarityArray(i)(j) = similarity(i)(j)
      }
    }

    //定义单位矩阵
    val iArray:Array[Array[Double]]=new Array(similarity.length)
    for(i <- 0 to iArray.length-1)
    {
      iArray(i)=new Array[Double](similarity.length)
      iArray(i)(i)=1.0
    }

    //定义度矩阵
    val DArray:Array[Array[Double]]=new Array(similarity.length)
    for(i <- 0 to similarity.length-1)
    {
      DArray(i)=new Array[Double](similarity.length)
    }

    for(i <- 0 to similarity.length-1)
    {
      DArray(i)(i)=Math.pow(similarity(i).toArray.sum,(-0.5))
    }
    //定义并计算非规范拉普拉斯矩阵
    val laplacian:Array[Array[Double]]=new Array(similarity.length)
    for(i <- 0 to laplacian.length-1)
    {
      laplacian(i)=new Array[Double](similarity.length)
    }
    for(i <- 0 to laplacian.length-1)
    {
      for(j <- 0 to laplacian(i).length-1)
      {
        laplacian(i)(j)=DArray(i)(j)-similarity(i)(j)
      }
    }

    //计算获取规范拉普拉斯矩阵
    val SMatrix=new DenseMatrix(similarityArray.length,similarityArray.length,similarityArray.reduceRight(_++_))
    val LMatrix=new DenseMatrix(laplacian.length,laplacian.length,laplacian.reduceRight(_++_))
    val DMatrix=new DenseMatrix(DArray.length,DArray.length,DArray.reduceRight(_++_))
    val IMatrix=new DenseMatrix(iArray.length,iArray.length,iArray.reduceRight(_++_))
    val finalLaplacian=IMatrix.-(DMatrix.*(SMatrix).*(DMatrix))//规范拉普拉斯矩阵的计算公式

    //拉普拉斯矩阵的数组转换为Array[Array[Double]]
    val finalLaplacianDataArray=finalLaplacian.data
    val finalLaplacianArray:Array[Array[Double]]=new Array(laplacian.length)
    for(i <- 0 to finalLaplacianArray.length-1)
    {
      finalLaplacianArray(i)=new Array[Double](laplacian(0).length)
    }
    var startOfDataArray:Int=0
    for(i <- 0 to finalLaplacianArray(0).length-1)
    {
      for(j <- 0 to finalLaplacianArray.length-1)
      {
        finalLaplacianArray(j)(i)=BigDecimal(finalLaplacianDataArray(startOfDataArray)).setScale(10,BigDecimal.RoundingMode.HALF_UP).toDouble
        startOfDataArray=startOfDataArray+1
      }
    }
    finalLaplacianArray//返回最后的拉普拉斯矩阵
  }

  /**
    * 计算矩阵的特征值和特征向量
    * @param laplacian 规范的拉普拉斯矩阵
    * @return 特征值和特征向量
    */
  def getEigenvalueAndEigenvector(laplacian:Array[Array[Double]])=
  {
    val lMatrix=new DenseMatrix(laplacian.length,laplacian.length,laplacian.reduceRight(_++_))
    eigSym(lMatrix)//breeze里面有的计算特征值和特征响亮工具
  }

  /**
    * 计算特征向量间距离
    * @param target1 特征向量1Array[Double]
    * @param target2 特征向量2Array[Double]
    *                特征向量1，2等长
    * @param maxAndMinOfEachCol 特征向量矩阵每列的最大和最小值
    * @return 特征向量间的距离
    */
  def calculateDistanceOfEigen(target1:Array[Double],target2:Array[Double],maxAndMinOfEachCol:ArrayBuffer[ArrayBuffer[Double]]): Double =
  {
    var sum:Double=0.0
    for(i <- 0 to target1.length-1)
    {
      sum+=Math.pow(((target1(i)-target2(i))/(maxAndMinOfEachCol(i)(0)-maxAndMinOfEachCol(i)(1))),2)//特征向量间距离计算公式
    }
    Math.sqrt(sum)
  }

  /**
    * 计算特征向量集的熵
    * @param similarityOfEigen 特征向量相似度集合
    * @param eliminateNumber 被筛除掉的特征向量编号
    * @return 特征向量集的熵
    */
  def calculateEntropy(similarityOfEigen:Array[Array[Double]],eliminateNumber:Int): Double =
  {
    var result:Double=0.0
    if(eliminateNumber==(-1))//如果编号为-1则计算全集的熵
    {
      var sum:Double=0.0
      for(i <- 0 to similarityOfEigen.length-1)
      {
        for(j <- 0 to similarityOfEigen(i).length-1)
        {
          if(i!=j)
          {
            sum += similarityOfEigen(i)(j) * Math.log(similarityOfEigen(i)(j)) + (1 - similarityOfEigen(i)(j)) * Math.log(1 - similarityOfEigen(i)(j))
          }
        }
      }
      result=sum*(-1.0)/((similarityOfEigen.length)*(similarityOfEigen.length))
    }
    else//如果编号不为-1则计算除掉编号之后的
    {
      var sum:Double=0.0
      for(i <- 0 to similarityOfEigen.length-1)
      {
        for(j <- 0 to similarityOfEigen(i).length-1)
        {
          if(i!=eliminateNumber&&j!=eliminateNumber)
          {
            if(i!=j)
            {
              sum += similarityOfEigen(i)(j) * Math.log(similarityOfEigen(i)(j)) + (1 - similarityOfEigen(i)(j)) * Math.log(1 - similarityOfEigen(i)(j))
            }
          }
        }
      }
      result=sum*(-1.0)/((similarityOfEigen.length-1)*(similarityOfEigen.length-1))
    }
    result
  }

  /**
    * 利用基于熵的最大特征向量选择方法选择CLUSTERNUM个特征向量
    * @param laplacian 规范的拉普拉斯矩阵
    * @return 筛选出CLUSTERNUM个拉普拉斯矩阵的特征向量
    */
  def getEigenVectorsFromLaplalcian(laplacian:Array[Array[Double]]):ArrayBuffer[ArrayBuffer[Double]]=
  {
    val eigenValueAndEigenvector=getEigenvalueAndEigenvector(laplacian)//得到从小到大排列的拉普拉斯矩阵的特征值以及对应的特征向量
    val eigenVectorRows=eigenValueAndEigenvector.eigenvectors.rows//特征向量的行数
    val eigenVectorCols=eigenValueAndEigenvector.eigenvectors.cols//特征向量的个数
    val eigenVectorDataArray=eigenValueAndEigenvector.eigenvectors.data//将特征向量转换为一个一维的数组来

    //定义并初始化eigenVectorArray,并将列向量转化为行向量
    val eigenVectorArray:ArrayBuffer[ArrayBuffer[Double]]=new ArrayBuffer[ArrayBuffer[Double]]()//用于存储最后提取的特征向量
    for(i <- 0 to eigenVectorRows-1)
    {
      val temp:ArrayBuffer[Double]=new ArrayBuffer[Double]()
      for(j <- 0 to eigenVectorCols-1)
      {
        temp+=eigenVectorDataArray(i*eigenVectorRows+j)
      }
      eigenVectorArray+=temp
    }
    //定义maxAndMinOfEachCol存储特征向量矩阵每一列的最大和最小值,即取每一列的最大和最小值
    val maxAndMinOfEachCol:ArrayBuffer[ArrayBuffer[Double]]=new ArrayBuffer[ArrayBuffer[Double]]()
    for(i <- 0 to eigenVectorArray(0).length-1)
    {
      var tempMax:Double=Double.MinValue
      var tempMin:Double=Double.MaxValue
      var temp:ArrayBuffer[Double]=new ArrayBuffer[Double]()
      for(j <- 0 to eigenVectorArray.length-1)
      {
        if(eigenVectorArray(j)(i)>=tempMax)
        {
          tempMax=eigenVectorArray(j)(i).toDouble
        }
        if(eigenVectorArray(j)(i)<=tempMin)
        {
          tempMin=eigenVectorArray(j)(i).toDouble
        }
      }
      temp+=tempMax
      temp+=tempMin
      maxAndMinOfEachCol+=temp
    }
    //计算特征向量两两之间距离
    val distanceOfEigen:Array[Array[Double]]=new Array(eigenVectorRows)
    for(i <- 0 to distanceOfEigen.length-1)
    {
      distanceOfEigen(i)=new Array[Double](distanceOfEigen.length)
    }
    for(i <- 0 to distanceOfEigen.length-1)
    {
      for(j <- 0 to distanceOfEigen.length-1)
      {
        if(i==j)
        {
          distanceOfEigen(i)(j)=0
        }
        else
        {
          if (distanceOfEigen(i)(j) == 0)
          {
            distanceOfEigen(i)(j) = calculateDistanceOfEigen(eigenVectorArray(i).toArray, eigenVectorArray(j).toArray, maxAndMinOfEachCol)
            distanceOfEigen(j)(i) = distanceOfEigen(i)(j)
          }
        }
      }
    }
    //定义征并初始化特征向量相似度矩阵,通过公式Sij =e^(−disij)计算计算特征向量相似度
    val similarityOfEigen:Array[Array[Double]]=new Array(eigenVectorRows)
    for(i <- 0 to similarityOfEigen.length-1)
    {
      similarityOfEigen(i)=new Array[Double](distanceOfEigen.length)
    }
    for(i <- 0 to similarityOfEigen.length-1)
    {
      for(j <- 0 to similarityOfEigen(i).length-1)
      {
        if(i==j)
        {
          similarityOfEigen(i)(j)=0
        }
        else
        {
          similarityOfEigen(i)(j) = Math.pow(Math.E, -(distanceOfEigen(i)(j)))
          similarityOfEigen(j)(i) = similarityOfEigen(i)(j)
        }
      }
    }
    //计算重要性度量I(vi) = E(V- vi) – E(V)
    val selectedCollection:java.util.TreeMap[Double,Int]=new java.util.TreeMap[Double,Int]()//用于从大到小存储重要性度量为正数的数据
    val EV:Double=calculateEntropy(similarityOfEigen,-1)//计算全集时的熵
    for(i <- 0 to similarityOfEigen.length-1)
    {
      val EV_Vi:Double=calculateEntropy(similarityOfEigen,i)
      if(EV_Vi-EV>0)//如果重要性度量大于0，则放入树中
      {
        selectedCollection.put(EV_Vi-EV,i)
      }
    }
    //取树中前6个最大的特征向量标号放入treeArray中
    val indexArray:ArrayBuffer[Int]=new ArrayBuffer[Int]()
    val selectedCollectionArray=selectedCollection.keySet().toArray
    for(i <- 0 to CLUSTERNUM-1)
    {
      val key=selectedCollectionArray((selectedCollection.keySet().size() - (1+i)))
      indexArray+=selectedCollection.get(key)
    }
    //取出6个最大的特征向量放入eigenVectorArrayBuffer(6*n)(n为原始数据中共有多少支股票)
    val eigenVectorArrayBuffer:ArrayBuffer[ArrayBuffer[Double]]=new ArrayBuffer[ArrayBuffer[Double]]()
    for(i <- 0 to indexArray.length-1)
    {
      eigenVectorArrayBuffer+=eigenVectorArray(indexArray(i))
    }
    //转置矩阵eigenVectorArrayBuffer,得到最后的目标集(n*6)
    val eigenVectorArrayBufferTranspose:ArrayBuffer[ArrayBuffer[Double]]=new ArrayBuffer[ArrayBuffer[Double]]()
    for(i <- 0 to eigenVectorArrayBuffer(0).length-1)
    {
      val temp:ArrayBuffer[Double]=new ArrayBuffer[Double]()
      for(j <- 0 to eigenVectorArrayBuffer.length-1)
      {
        temp+=eigenVectorArrayBuffer(j)(i)
      }
      eigenVectorArrayBufferTranspose+=temp
    }
    //返回转置后的矩阵
    eigenVectorArrayBufferTranspose
  }

  /**
    * 计算两个维度相等的向量的欧式距离
    * @param target1 第一个向量Array[Double]
    * @param target2 第二个向量Array[Double]
    * @return 两个向量的欧式距离
    */
  def calculateDistanceToAveragePoint(target1:Array[Double],target2:Array[Double]):Double=
  {
    var temp:Double=0.0
    for(i <- 0 to target2.length-1)
    {
      temp+=Math.pow(target2(i)-target1(i),2).toDouble
    }
    Math.sqrt(temp)
  }

  /**
    * 打印出聚类的结果
    * @param eigenVectorArrayBuffer 提取出的特征向量
    */
  def printClusterResult(eigenVectorArrayBuffer:ArrayBuffer[ArrayBuffer[Double]]): Unit =
  {
    //定义并初始化Y矩阵
    val YArray:Array[Array[Double]]=new Array(eigenVectorArrayBuffer.length)
    for(i <- 0 to eigenVectorArrayBuffer.length-1)
    {
      YArray(i)=new Array[Double](eigenVectorArrayBuffer(i).length)
    }
    //Y矩阵赋值后将每行向量转变为单位向量
    for(i <- 0 to eigenVectorArrayBuffer.length-1)
    {
      var sum:Double=0
      for(k <- 0 to eigenVectorArrayBuffer(i).length-1)
      {
        sum+=Math.pow(eigenVectorArrayBuffer(i)(k),2)
      }
      for(j <- 0 to eigenVectorArrayBuffer(i).length-1)
      {
        YArray(i)(j)=eigenVectorArrayBuffer(i)(j)/Math.sqrt(sum)
      }
    }
    //计算出6个类的平均点(虚拟中心点)
    val averagePoint:ArrayBuffer[Array[Double]]=new ArrayBuffer[Array[Double]]()
    for(i <- 0 to CLUSTERNUM-1)
    {
      var sum:Array[Double]=new Array(YArray(0).length)
      for(j <- i*STANDARD_NUM_OF_EACH_GROUP to i*STANDARD_NUM_OF_EACH_GROUP+STANDARD_NUM_OF_EACH_GROUP-1)
      {
        val temp=for(k <- 0 to YArray(j).length-1)yield(sum(k)+YArray(j)(k))
        sum=temp.toArray
      }
      averagePoint+=(for(k <- 0 to sum.length-1) yield (sum(k)/60)).toArray
    }
    //计算实际每个点到虚拟中心点的距离
    val distanceToAverage:ArrayBuffer[Double]=new ArrayBuffer[Double]()
    for(i <- 0 to CLUSTERNUM)
    {
      for(j <- i*STANDARD_NUM_OF_EACH_GROUP to i*STANDARD_NUM_OF_EACH_GROUP+STANDARD_NUM_OF_EACH_GROUP-1)
      {
        distanceToAverage+=calculateDistanceToAveragePoint(YArray(j),averagePoint(i)).toDouble
      }
    }
    //根据每个点到虚拟中心点的距离,取距离最小的点为每一类的中心点
    var centerPoint1:Int=0
    var centerPoint1Min:Double=Double.MaxValue
    var centerPoint2:Int=0
    var centerPoint2Min:Double=Double.MaxValue
    var centerPoint3:Int=0
    var centerPoint3Min:Double=Double.MaxValue
    var centerPoint4:Int=0
    var centerPoint4Min:Double=Double.MaxValue
    var centerPoint5:Int=0
    var centerPoint5Min:Double=Double.MaxValue
    var centerPoint6:Int=0
    var centerPoint6Min:Double=Double.MaxValue
    for(i <- 0 to distanceToAverage.length-1)
    {
      if(i<=(0*STANDARD_NUM_OF_EACH_GROUP+STANDARD_NUM_OF_EACH_GROUP-1)&&i>=0)
      {
        if(distanceToAverage(i)<centerPoint1Min)
        {
          centerPoint1Min=distanceToAverage(i)
          centerPoint1=i
        }
      }
      else if(i<=(1*STANDARD_NUM_OF_EACH_GROUP+STANDARD_NUM_OF_EACH_GROUP-1)&&i>=1*STANDARD_NUM_OF_EACH_GROUP)
      {
        if(distanceToAverage(i)<centerPoint2Min)
        {
          centerPoint2Min=distanceToAverage(i)
          centerPoint2=i
        }
      }
      else if(i<=(2*STANDARD_NUM_OF_EACH_GROUP+STANDARD_NUM_OF_EACH_GROUP-1)&&i>=2*STANDARD_NUM_OF_EACH_GROUP)
      {
        if(distanceToAverage(i)<centerPoint3Min)
        {
          centerPoint3Min=distanceToAverage(i)
          centerPoint3=i
        }
      }
      else if(i<=(3*STANDARD_NUM_OF_EACH_GROUP+STANDARD_NUM_OF_EACH_GROUP-1)&&i>=3*STANDARD_NUM_OF_EACH_GROUP)
      {
        if(distanceToAverage(i)<centerPoint4Min)
        {
          centerPoint4Min=distanceToAverage(i)
          centerPoint4=i
        }
      }
      else if(i<=(4*STANDARD_NUM_OF_EACH_GROUP+STANDARD_NUM_OF_EACH_GROUP-1)&&i>=4*STANDARD_NUM_OF_EACH_GROUP)
      {
        if(distanceToAverage(i)<centerPoint5Min)
        {
          centerPoint5Min=distanceToAverage(i)
          centerPoint5=i
        }
      }
      else if(i<=(5*STANDARD_NUM_OF_EACH_GROUP+STANDARD_NUM_OF_EACH_GROUP-1)&&i>=5*STANDARD_NUM_OF_EACH_GROUP)
      {
        if(distanceToAverage(i)<centerPoint6Min)
        {
          centerPoint6Min=distanceToAverage(i)
          centerPoint6=i
        }
      }
    }
    //根据中心点分类并打印结果
    for(i <- STANDARD_NUM_OF_EACH_GROUP*CLUSTERNUM to YArray.length-1)
    {
      val sixDistance:ArrayBuffer[Double]=new ArrayBuffer[Double]()
      sixDistance+=calculateDistanceToAveragePoint(YArray(i),YArray(centerPoint1))
      sixDistance+=calculateDistanceToAveragePoint(YArray(i),YArray(centerPoint2))
      sixDistance+=calculateDistanceToAveragePoint(YArray(i),YArray(centerPoint3))
      sixDistance+=calculateDistanceToAveragePoint(YArray(i),YArray(centerPoint4))
      sixDistance+=calculateDistanceToAveragePoint(YArray(i),YArray(centerPoint5))
      sixDistance+=calculateDistanceToAveragePoint(YArray(i),YArray(centerPoint6))

      var minOfSixValue:Double=Double.MaxValue
      var minOfSixPosition:Int=0
      for(j <- 0 to sixDistance.length-1)
      {
        if(sixDistance(j)<minOfSixValue)
        {
          minOfSixValue=sixDistance(j)
          minOfSixPosition=j+1
        }
      }
      println(minOfSixPosition)
    }
  }

  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName(APPNAME).setMaster(MASTERURL).set(PROPERTY_1_TYPE, PROPERTY_1_VALUE).setJars(JARSPATH)//设置conf参数
    val sc=new SparkContext(conf)//定义并初始化SparkContext

    val originalData:RDD[String]=readData_HDFS(ORIGINALFILEPATH,sc)//获取原始数据
    val pretreatedOriginalData:RDD[(String, List[(Double, Long)])]=pretreatOriginalData(originalData)//对原始数据预处理
    val distanceAndSigma:(Array[Array[Double]],Array[Double])=getDistanceAndSigmaOfEveryTwoStocks(pretreatedOriginalData)//获取DTW距离和Sigma数组
    val dtwDistance:Array[Array[Double]]=distanceAndSigma._1//dtwDistance数组
    val sigma:Array[Double]=distanceAndSigma._2//sigma数组
    val similarity:ArrayBuffer[ArrayBuffer[Double]]=calculateSimilarity(dtwDistance,sigma)//计算出相似度矩阵
    val laplacianMatrix:Array[Array[Double]]=getLaplacianMatrix(similarity)//计算出拉普拉斯矩阵
    val eigenVectorArrayBuffer:ArrayBuffer[ArrayBuffer[Double]]=getEigenVectorsFromLaplalcian(laplacianMatrix)//提取特征向量
    printClusterResult(eigenVectorArrayBuffer)//计算出聚类结果(分类)


//    实时接收数据并处理(流式接收数据并处理)
//    var result:RDD[(String,List[(Double,Long)])]=null
//    val ssc = new StreamingContext(conf, Seconds(60))
//    val lines = ssc.socketTextStream(DATA_SERVER_IP, DATA_SERVER_PORT)
//    val processResult=lines.map(line =>
//    {
//      var processLine:String=null
//      if(!line.substring(1,line.length()).startsWith("{"))
//      {
//        processLine="{"+line.substring(1,line.length())
//      }
//      else
//      {
//        processLine=line.substring(1,line.length())
//      }
//      val json=parse(""+processLine.toString+"")
//      val dataList:List[(String,BigInt,String)]=for
//      {
//        JObject(child) <- json
//        JField("code",JString(code)) <- child
//        JField("dateTime",JInt(dateTime)) <- child
//        JField("currentPrice",JString(currentPrice)) <- child
//      }yield (code,dateTime,currentPrice)
//      (dataList(0)._1.toString,List((dataList(0)._3.toDouble,dataList(0)._2.toLong)))
//    }).reduceByKey((_++_))
//    processResult.foreachRDD {
//      rdd =>
//        if(result==null)
//        {
//          if(rdd.collect().length<=0)
//          {
//            println("result==null")
//            result = null
//            result.collect().foreach {println}
//          }
//          else
//          {
//            println("result!=null")
//            result=rdd
//          }
//        }
//        else
//        {
//          println("result!=null")
//          result=result.union(rdd)
//          result=result.reduceByKey(_:::_)
//        }
//    }

       //实时接收数据并处理(Redis)
//     var priceList:ArrayBuffer[String]=new ArrayBuffer[String]()
//     var jd = new Jedis(Your Redis IP, Port)//这里填写需要提取数据的IP地址和端口号
//     for(code <- 0 to n)//n代表的是抓取数据的条数
//     {
//       var dataStr = jd.get(code)
//       pricelist += dataStr
//     }
//    val originalData = sc.parallelize(pricelist.toArray)

  }
}
