package test;
import java.io.BufferedReader;

import java.io.IOException;

import java.io.InputStreamReader;

import java.net.HttpURLConnection;

import java.net.URL;

import java.net.URLConnection;



import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;

import org.apache.spark.ml.feature.CountVectorizerModel;

import org.apache.spark.ml.feature.IDFModel;

import org.apache.spark.sql.DataFrame;

import org.apache.spark.sql.Row;

import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.SQLContext;

import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.types.Metadata;

import org.apache.spark.sql.types.StructField;

import org.apache.spark.sql.types.StructType;

import org.json.JSONArray;

import org.json.JSONObject;


public class Read_Demo {

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("update_db").set("spark.sql.parquet.binaryAsString", "true");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		jsc.setLogLevel("ERROR");

		SQLContext sqlContext = new SQLContext(jsc);

		DataFrame df = sqlContext.read().load(args[0], args[1]);



		df.registerTempTable("data");

		DataFrame url = sqlContext.sql(

				"select distinct domain , path, concat(domain,path) as url from data where  domain in (\"cafef.vn\",\"dantri.com.vn\",\"kenh14.vn\",\"soha.vn\",\"cafebiz.vn\",\"genk.vn\",\"afamily.vn\")");

		JavaRDD<Row> url_rdd = url.javaRDD().map(new Function<Row, Row>() {

			public String[] makeGetRequest(String stringUrl_query) throws IOException {

				String [] results = new String[3];

				try {

					String line = "";

					while (line.length() == 0) {

						URL url = new URL("http://192.168.23.189:5000/search_detail");

						URLConnection con = url.openConnection();

						HttpURLConnection httpCon = (HttpURLConnection) con;

						httpCon.setRequestMethod("GET");

						httpCon.setRequestProperty("url", stringUrl_query);

						BufferedReader br = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));

						while (br.ready()) {

							line += br.readLine();

						}

						br.close();

					}

					String result = "-2";

					JSONObject json = new JSONObject(line);

					JSONArray arr = json.getJSONArray("list_cate");

					if (arr.length() != 0) {

						String[] object = arr.getString(0).split(":");	

						result = object[0];

						results[2] = object[1];

						

					} else {

						result = json.getString("reason_code");

					}

					results[0] = result;

					results[1] = json.getString("content");

					return results;

				} catch (Exception e) {

					e.printStackTrace();

					return results;

				}

			}



			@Override

			public Row call(Row row) throws Exception {

				String stringUrl_query = "http://" + row.getString(2).trim();

				String[] results = makeGetRequest(stringUrl_query);

				Row kq = RowFactory.create(row.getString(0), row.getString(1), row.getString(2), results[0],results[2],results[1]);

				return kq;

			}

		});

		StructType schema = new StructType(

				new StructField[] { new StructField("domain", DataTypes.StringType, true, Metadata.empty()),

						new StructField("path", DataTypes.StringType, true, Metadata.empty()),

						new StructField("url", DataTypes.StringType, true, Metadata.empty()),

						new StructField("cate", DataTypes.StringType, true, Metadata.empty()),

						new StructField("percent", DataTypes.StringType, true, Metadata.empty()),

						new StructField("raw", DataTypes.StringType, true, Metadata.empty())});

		DataFrame d4 = sqlContext.createDataFrame(url_rdd, schema);

		CountVectorizerModel cvModel = getCvModel(args[3]);

		IDFModel idfModel = getIdfModel(args[4]);

		DataFrame tfidf = idfModel.transform(cvModel.transform(d4));

		tfidf.write().parquet(args[2]);

		



	}

	

	public static CountVectorizerModel getCvModel(String cvModelPath) throws Exception { 

		return CountVectorizerModel.load(cvModelPath);

	}

	public static IDFModel getIdfModel(String idfModelPath) throws Exception { 

		return IDFModel.load(idfModelPath);

	}

}
