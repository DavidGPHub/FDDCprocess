package com.entrobus

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext, ml}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DateType, DoubleType}


object Anti_Fraud {

  val datapath = "E:\\Spark\\ant\\sample\\"
//  val datapath = "hdfs://10.18.0.4:8020/user/fanli/ant/"

//  val runningmode = "real"
  val runningmode = "test"

//  val model = "lr"
  val model = "mlpc"
//  val model = "gbt"
//  val model = "rf"
//  val model = "dt"

//  val model = "lsvc"  //SVM only has category no probability


  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("Anti_Fraud__" + model)
//      .setMaster("local[*]")
//      .set("log4j.rootCategory", "ERROR")
//      .set("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=3048m")
//      .set("spark.executor.extraJavaOptions", "-XX:ReservedCodeCacheSize=2548m")
//    val sc = new SparkContext(conf).setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("Spark Anti_Fraud data __" + model)
      .getOrCreate()

    spark.sparkContext.getConf
      .setAppName("Anti_Fraud")
      .set("spark.driver.maxResultSize", "3g")
      .set(" spark.scheduler.mode", "FAIR")
      .set("log4j.rootCategory", "ERROR")
      .set("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=2148m")
      .set("spark.executor.extraJavaOptions", "-XX:ReservedCodeCacheSize=1848m")

//    spark.conf.set("spark.driver.maxResultSize", "3g")
//    spark.conf.set(" spark.scheduler.mode", "FAIR")

    println(NowDate() + "START =================")
    adFeatureData(spark)
    println(NowDate() + "COMPLETE =================")


    spark.stop()

  }

  private def adFeatureData(spark: SparkSession): Unit = {
    import spark.implicits._
    var startTime = System.nanoTime()

    val ant_test_DF = spark.read.format("csv")
      .option("header", "true")
      .option("inferScheme", "true")
      .load(datapath + "atec_anti_fraud_test_a.csv")

    val ant_test_DF01 = ant_test_DF.withColumn("label", lit(-999))
    val test01 = ant_test_DF01.select(ant_test_DF01.col("id"),
      ant_test_DF01.col("label"), ant_test_DF01.col("*") )
    val test_col = test01.columns.toBuffer
    val test_col02 = test_col.remove(test_col.size - 1)
    val test_col03 = test_col.remove(2)
    println(test_col.toArray.mkString(", "))
    ant_test_DF01.createOrReplaceTempView("test03")
    val test04 = spark.sql("select " + test_col.toArray.mkString(", ") +
      " from test03")

    test04.printSchema()
    test04.show(3,false)


    val ant_traing_DF = spark.read.format("csv")
      .option("header", "true")
      .option("inferScheme", "true")
      .load(datapath + "atec_anti_fraud_train.csv")

//    ant_test_DF.printSchema()
    test04.show(10,false)
//    ant_traing_DF.printSchema()
    ant_traing_DF.show(11,false)

    val train_union_test = ant_traing_DF.union(test04)
    println("===================total train + testing is : " + train_union_test.count())

//    val cols = train_union_test.columns
    //convert label to Double, and convert date to DateType

//    val cols_indexer = cols.filter(_.contains("f")).map(
////      col => new StringIndexer().setInputCol(col).setOutputCol(s"${col}_ind")
//      col => ant_traing_DF.col(col).cast(DoubleType).as(col)
//    )

    val conv01 = train_union_test.select(train_union_test.col("label").cast(DoubleType).as("label_cast"),
      train_union_test.col("*") )
//    conv01.printSchema()
    conv01.show(2,false)

    conv01.createOrReplaceTempView("conv03")


    spark.udf.register("str2double", (str:String ) => parseDouble02(str))

//    val indexer = udf { index: String => parseDouble02(index) }
//    val conv02 = conv01.select($"id", UDF("str2double", $"*"))
//    val res = spark.sql("select id, str2double(f5) as f5_ind from conv03")
//    res.show(7,false)

    var cols01 = conv01.columns
    println(cols01.mkString(", "))

    val sqlstr = cols01.filter(_.contains("f")).map(t => s" str2double($t) as ${t}_ind ").mkString(",")
    println(sqlstr + "===============================")

    val res02 = spark.sql(s"select id, label_cast, substring(date,0,6) as yearmonth ," +
      s"substring(date,7,2) as day, $sqlstr from conv03")
    res02.show(8,false)

//    conv02.printSchema()
//    conv02.show(5,false)


    val cols02 = res02.columns
    val cols_ind_list = cols02.filter(_.contains("_ind"))
    println(cols_ind_list.mkString(",  "))

//    val assembler = new VectorAssembler()
//      .setInputCols(cols_ind_list)
//      .setOutputCol("rawFeatures")
//        .transform(res02)

//    assembler.printSchema()


    println("===========================COL Descirbe statics")
//    val conv01_des_01 = conv01.describe("*")
//    conv01_des_01.printSchema()
//    conv01_des_01.show(20, false)


//    val conv02 = conv01.na.fill(0.0)

//    println("===========================COL Descirbe statics AFTER FILL with 0.0 =============")
    val conv01_des_02 = res02.describe("*")
//    conv01_des_02.printSchema()
    conv01_des_02.show(20,false)

    println("===========================COL list after union and assembler =============")
    val cols_all = conv01_des_02.columns
    println(cols_all.mkString(", "))

//    val col_quan = new ArrayBuffer[String]()

    val col_quan = cols_all.filter(p => p.contains("_ind") && (conv01_des_02.filter("summary = 'max' ")
      .where(s"$p > 10").count() >= 1))
    val col_vec  = cols_all.filter(p => p.contains("_ind") && !col_quan.contains(p))

    println("column for quan : " + col_quan.mkString(", "))
    println("column left for vec : " + col_vec.mkString(", ") + " ---------------------")

    val col_quan_cnv = col_quan.map(_.concat("_quan"))
    val col_vec_cnv = col_vec++col_quan_cnv

    var elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"load train/test file  time: $elapsedTime seconds -----------")
    println("------------------start to read and convert userFeature -----------------")
    startTime = System.nanoTime()

//    val cols_indexer : Array[org.apache.spark.ml.PipelineStage] = cols.filter(_.contains("f")).map(
//            col => new StringIndexer()
//              .setInputCol(col)
//              .setOutputCol(s"${col}_ind")
//              .setHandleInvalid("keep")
//    )

    val quan_indexer : Array[org.apache.spark.ml.PipelineStage] = col_quan.map(
      col => new QuantileDiscretizer()
        .setInputCol(col)
        .setOutputCol(s"${col}_quan")
//        .setNumBuckets(8)//设置分箱数
        .setNumBuckets(6)//设置分箱数
        .setRelativeError(0.1)
    )

//    val vector_indexer : Array[org.apache.spark.ml.PipelineStage] = col_vec.map(
//      col => new VectorIndexer()
//        .setInputCol(col)
//        .setMaxCategories(6)
//        .setOutputCol(s"${col}_vec")
//    )

    val assembler02 = new VectorAssembler()
      .setInputCols(col_vec_cnv)
      .setOutputCol("rawFeatures01")
//      .transform(res02)

    val vector_indexer = new VectorIndexer()
            .setInputCol("rawFeatures01")
            .setMaxCategories(6)
            .setOutputCol("rawFeatures02")
//      .fit(res02)


//    val res03 = new QuantileDiscretizer()
//      .setInputCol("f5_ind")
//      .setOutputCol("f5_ind_quan")
//      .setNumBuckets(6)
//      .setRelativeError(0.1)
//      .fit(assembler)
//      .transform(assembler)

//    val res04 =  new VectorIndexer()
//      .setInputCol("f5_ind_quan")
//      .setMaxCategories(6)
//      .setOutputCol("f5_ind_quan02")
//      .fit(res03)
//      .transform(res03)

//    res03.printSchema()
//    res03.show(6, false)

//    val transfer_Pipeline = new Pipeline().setStages(vector_indexer++quan_indexer)
//
//    val conv03 = transfer_Pipeline.fit(res02).transform(res02)
//    conv03.printSchema()
//    conv03.show(9,false)

/*
    //定义输入输出列和最大类别数为5，某一个特征
      //（即某一列）中多于5个取值视为连续值
      VectorIndexerModel featureIndexerModel=new VectorIndexer()
      .setInputCol("features")
      .setMaxCategories(5)
      .setOutputCol("indexedFeatures")
      .fit(rawData);

      分位树为数离散化，和Bucketizer（分箱处理）一样也是：将连续数值特征转换为离散类别特征。
      实际上Class QuantileDiscretizer extends （继承自） Class（Bucketizer）。

      参数1：不同的是这里不再自己定义splits（分类标准），而是定义分几箱(段）就可以了。
      QuantileDiscretizer自己调用函数计算分位数，并完成离散化。
      -参数2： 另外一个参数是精度，如果设置为0，则计算最精确的分位数，这是一个高时间代价的操作。
      另外上下边界将设置为正负无穷，覆盖所有实数范围。

      new QuantileDiscretizer()
             .setInputCol("ages")
             .setOutputCol("qdCategory")
             .setNumBuckets(4)//设置分箱数
             .setRelativeError(0.1)//设置precision-控制相对误差
             .fit(df)
             .transform(df)
             .show(10,false);
*/


    //    val ind01 = cols_indexer(0).asInstanceOf[StringIndexer]
//    ind01.fit(conv01).transform(conv01).printSchema()

//    val colindexer = new StringIndexer().setInputCol("f1").setOutputCol("f1_ind")
//    colindexer
//    val colindexer01 = colindexer.fit(ant_traing_DF)
//    val colindexer02 = colindexer01.transform(ant_traing_DF)
//    colindexer02.printSchema()
//    colindexer02.show(3,false)

//    cols.map(
//    )

//    println("====================CONV02 schema")
//    val conv02 = assembler.transform(conv01)
//    conv02.printSchema()
//    conv02.show(4,false)
//
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(6)
//      .fit(res02)

    val train_conv01 = res02.filter("label_cast >= 0")
    val test_conv01 = res02.filter("label_cast < -1")
    println("=============total Train set is : " + train_conv01.count())
    println("=============total Test set is : " + test_conv01.count())


//
//    val slicer = new VectorSlicer().setInputCol("rawFeatures").setOutputCol("slicedfeatures").setNames(Array("f1_ind"))
//    val scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("features").setWithStd(true).setWithMean(true)
    val scaler = new StandardScaler().setInputCol("rawFeatures02").setOutputCol("rawFeatures03").setWithStd(true).setWithMean(true)
    val normalizer = new Normalizer().setInputCol("rawFeatures03").setOutputCol("features")
////    val binarizerClassifier = new Binarizer().setInputCol("ArrDelay").setOutputCol("binaryLabel").setThreshold(15.0)
    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.0)
      .setElasticNetParam(0.0)
      .setTol(1E-6)
      .setFitIntercept(true)
      .setLabelCol("label_cast")
      .setFeaturesCol("features")


    val gbt = new GBTClassifier()
      .setLabelCol("label_cast")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")


    val rf = new RandomForestClassifier()
      .setLabelCol("label_cast")
      .setFeaturesCol("features")
      .setNumTrees(10)

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label_cast")
      .setFeaturesCol("features")

    val lsvc = new LinearSVC()
      .setMaxIter(20)
      .setRegParam(0.1)
      .setLabelCol("label_cast")
      .setFeaturesCol("features")



    // 隐藏层结点数=2n+1，n为输入结点数
    // 指定神经网络的图层：输入层n个节点(即n个特征)；两个隐藏层，隐藏结点数分别为n+1和n；输出层2个结点(即二分类)
    val layers = Array[Int](297, 158, 68, 2)


    // 建立多层感知器分类器MLPC模型
    // 传统神经网络通常，层数<=5，隐藏层数<=3
    // layers 指定神经网络的图层
    // MaxIter 最大迭代次数
    // stepSize 每次优化的迭代步长，仅适用于solver="gd"
    // blockSize 用于在矩阵中堆叠输入数据的块大小以加速计算。 数据在分区内堆叠。 如果块大小大于分区中的剩余数据，则将其调整为该数据的大小。 建议大小介于10到1000之间。默认值：128
    // initialWeights 模型的初始权重
    // solver 算法优化。 支持的选项：“gd”（minibatch梯度下降）或“l-bfgs”。 默认值：“l-bfgs”
    val mlpc = new MultilayerPerceptronClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label_cast")
      .setLayers(layers)
      .setMaxIter(200).setTol(1E-6).setSeed(1234L)
//    .setBlockSize(128).setSolver("l-bfgs")
//    .setInitialWeights(Vector).setStepSize(0.03)

    val tran_Pipeline = new Pipeline().setStages(quan_indexer++Array(assembler02, vector_indexer, scaler, normalizer))

    val now = NowDate()
    val data_transfer_model = tran_Pipeline.fit(res02)
    val res03 = data_transfer_model.transform(res02)
    res03.printSchema()
    res03.show(12,false)
//    res03.write.save(datapath + "ant_data_" + model + now)

    val lr_Pipeline = new Pipeline()

    model match {
      case "lr" => lr_Pipeline.setStages(Array(lr))
      case "gbt" => lr_Pipeline.setStages(Array(gbt))
      case "rf" => lr_Pipeline.setStages(Array(rf))
      case "dt" => lr_Pipeline.setStages(Array(dt))
      case "mlpc" => lr_Pipeline.setStages(Array(mlpc))
      case "lsvc" => lr_Pipeline.setStages(Array(lsvc))
    }

    val train_set = res03.filter("label_cast >= 0").toDF()
    val test_set = res03.filter("label_cast < -1").toDF()
    val blank_set = res03.filter("label_cast = -1")
    val blank_to1 = blank_set.withColumn("label_cast", blank_set.col("label_cast")*(-1))


    val train_09 = res03.where("yearmonth <= '201709'")
    val train_10 = res03.where("yearmonth > '201709' and yearmonth <= '201710'")
    val train_0910 = res03.where("yearmonth <= '201710' and label_cast >= 0")
    val train_11 = res03.where("yearmonth > '201710' or label_cast <= -1")

    println("train set has " + train_set.count() + " records, and test set has " + test_set.count() + " records =============")
    println("train0910 set has " + train_0910.count() + " records, and train_11 set has " + train_11.count() + " records =============")

    //all traing set + blank data convert to 1
      var train_set01 = train_set.union(blank_to1).toDF()
      var test_set01 = test_set
    if (runningmode.equals("test")) {
      train_set01 = train_0910
      test_set01 = train_11
    }

    val lrModel = lr_Pipeline.fit(train_set01)

    elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"model $model  Training  time: $elapsedTime seconds -----------")
    startTime = System.nanoTime()

    lrModel.save(datapath + model + "_Model_" + now)
    println("------------------model save to  -----------------"+ datapath + model + "_Model_" + now)
//    val predict = lrModel.transform(train_11)

    val metrics = new BinaryClassificationEvaluator()

    //real test set
    val predict = lrModel.transform(test_set01)
    predict.show(9,false)

//    val predict02 = predict.dropDuplicates("id")
//    predict02.printSchema()
//    println("---predict deduplicated---------" )
//    predict02.show(6,false)

    val predict03 = predict.select("id","label_cast",  "yearmonth","rawPrediction", "probability", "prediction")
    predict03.printSchema()
    predict03.show(6,false)
//    predict02.rdd.saveAsTextFile(datapath + "predict02_" + now)

    var predict01 = predict03
    if (runningmode.equals("test")) {
      predict01 = predict03.where("label_cast < -1")
    }
//    println("===================between 02 and 03=======")

//    predict03.write.option("path", datapath + "predict03_" + now).saveAsTable("predict03")
//    val predict04 = predict01.map(t => t.getAs[String]("id") + ", " +
//      t.getAs[DoubleType]("label_cast" )+ ", " +
//      t.getAs[DenseVector]("probability").toString() + ", " +
//      t.getAs[DoubleType]("prediction" )
//      )
//
//    predict04.write.text(datapath + model + "_predict04_" + now)
//    println("------------------predict save to  -----------------"+ datapath + model + "_predict04_" + now)

    elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"predict  time: $elapsedTime seconds -----------")

    val predict05 = predict01.map(t => t.getAs[String]("id") + ", " +
      t.getAs[DenseVector]("probability").toArray.apply(1) )
    predict05.show(4,false)
    predict05.write.text(datapath + model + "_predict05_" + now)
    println("------------------predict save to  -----------------"+ datapath + model + "_predict05_" + now)

    if (runningmode.equals("test")) {

        val predict06 = predict03.map(t => (t.getAs[String]("id"),
          t.getAs[Double]("label_cast"),
          t.getAs[DenseVector]("probability").toArray.apply(1)))
    //    val predict07 = predict06.select(predict06.col("_1").as("id"), predict06.col("_2").as("label_cast")
    //      ,predict06.col("_3").as("probability"))
        val predict07 = predict06.withColumnRenamed("_1", "id")
                                  .withColumnRenamed("_2", "label_cast")
                                  .withColumnRenamed("_3", "probability")


        predict07.printSchema()
        predict07.describe("*").show(false)
        predict07.groupBy("label_cast").count().show(false)
        predict07.show(4,false)

        //TPR1：当FPR等于0.001时的TPR
        //TPR2：当FPR等于0.005时的TPR
        //TPR3：当FPR等于0.01时的TPR
        //成绩 = 0.4 * TPR1 + 0.3 * TPR2 + 0.3 * TPR3


        val tpr1 = cal_TPR_FPR(predict07, 0.001)
        val tpr2 = cal_TPR_FPR(predict07, 0.005)
        val tpr3 = cal_TPR_FPR(predict07, 0.01)

        val score = 0.4 * tpr1 + 0.3 * tpr2 + 0.3 * tpr3

        println(s"  TPR1：当FPR等于0.001时的TPR : $tpr1")
        println(s"  TPR2：当FPR等于0.005时的TPR : $tpr2")
        println(s"  TPR3：当FPR等于0.01时的TPR : $tpr3")
        println(s"  成绩 = 0.4 * TPR1 + 0.3 * TPR2 + 0.3 * TPR3 = $score")
    }


//    val eva01 = new BinaryClassificationEvaluator()
//    val eva02 = eva01.evaluate()

//    startTime = System.nanoTime()


  }

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val date = dateFormat.format(now)
    date
  }

  def parseDouble02(s:String) = {
    parseDouble(s) match {
      case Some(t)=> t
      case None=> 0.0
    }
  }
  def parseDouble(s: String): Option[Double] = try { Some(s.toDouble) } catch { case _ => None }

  def cal_TPR_FPR(predict: DataFrame, fpr: Double): Double = {
    //TP： True Positives
    //FN：False Negatives
    //FP： False Positives
    //TN：True Negatives
    //
    //根据混淆矩阵的值可以算出FPR（False Positives rate）和TPR（True Positives Rate）
    //
    //TPR = TP / (TP + FN)
    //FPR = FP / (FP + TN)

    //TP
//    val tp = predict.where("label_cast = 1.0 and probability > threadhold")
    // TP + FN
    println("start to cal TPR for FPR ------------------" + fpr)

    val tpfn = predict.where("label_cast = 1.0 ")
    //FP
//    val fp = predict.where("label_cast = 0.0 and probability > threadhold")
    //FP + TN
    val fptn = predict.where("label_cast = 0.0 ")

//    var fpr = fp / fptn
//    var tpr = tp / tpfn
    println("total predict has records : " + predict.count())
    println("total predict has Positive records : " + tpfn.count() + "------------------")
    println("total predict has Negative records : " + fptn.count() + "------------------")

    //fpr = 0.001
    var count_fp = Math.ceil(fptn.count() * fpr).toInt
    if (count_fp < 1) {
      return 0.0
    }

    println("Math.ceil(fptn.count() * fpr) is " +   count_fp)


    val fp01 = fptn.orderBy(fptn.col("probability").desc)
//    val fp02 = fp01.withColumn("rn", rowNumber.over(fp01))
//      .where(s"rn = $count_fp")
    val fp03 = fp01.head(count_fp)
    println(s" first record is : ${fp03(0).getString(0)}  ${fp03(0).getDouble(1)}  ${fp03(0).getDouble(2)}")
    val threadhold = fp03(fp03.length - 1).getAs[Double]("probability")
    println("threadhold is -------------------" + threadhold)

//    val threadhold = fp02.groupBy("label_cast")
//      .max("probability")
//      .take(1)
//      .map(t => t.getAs[Double]("probability"))
//      .apply(0)

    //tpr
    val tp = predict.where(s"label_cast = 1.0 and probability > $threadhold")
    tp.show(2,false)
    println("tp is -----------------" + tp.count())
    val re = tp.count() * 1.0 / tpfn.count()
    re

  }





}
