package work6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

/*
 * 排除工资过低的职位**/
public class RecommenderFilterSalaryResult {

	final static int NEIGHBORHOOD_NUM = 2;
	final static int RECOMMENDER_NUM = 3;

	public static void main(String[] args) throws TasteException, IOException, ParseException {
		String file = "datafile/job/pv.csv";
		// 构建数据模型 GenericBooleanPrefDataModel
		DataModel dataModel = RecommendFactory.buildDataModelNoPref(file);

		// 指定距离（相似度）计算方法
		// 曼哈顿距离 CityBlockSimilarity
		// 指定距离最近的 NEIGHBORHOOD_NUM 2 个用户作为邻居
		RecommenderBuilder rb1 = RecommenderEvaluator.userCityBlock(dataModel);
		// 对数似然距离 LogLikelihoodSimilarity
		RecommenderBuilder rb2 = RecommenderEvaluator.itemLoglikelihood(dataModel);

		LongPrimitiveIterator iter = dataModel.getUserIDs();
		while (iter.hasNext()) {
			long uid = iter.nextLong();
			if (uid == 974) {
				//选择2种不同的算法进行比较
				System.out.print("userCityBlock    =>");
				filterSalaryHigherThanAvg8(uid, rb1, dataModel);
				System.out.print("itemLoglikelihood=>");
				filterSalaryHigherThanAvg8(uid, rb2, dataModel);
			}
		}
	}

	public static void filterSalaryHigherThanAvg8(long uid,
			RecommenderBuilder recommenderBuilder, DataModel dataModel)
			throws TasteException, IOException, ParseException {
		double uidAvgSalary = getAverageSalaryByUID("datafile/job/job.csv",uid, dataModel);
		System.out.println("Average Salary:" + uidAvgSalary);
		Set<Long> jobids = getFilterSalaryJobID("datafile/job/job.csv",uidAvgSalary);//exclusion list
        // 加入过滤规则
        // 如果包含该id则赋值为Double.NaN
        // 排除掉工资低于平均工资80%的职位
        // isFiltered(id) ? Double.NaN : originalScore;
		IDRescorer rescorer = new JobRescorer(jobids);
		List<RecommendedItem> list = recommenderBuilder.buildRecommender(
				dataModel).recommend(uid, RECOMMENDER_NUM, rescorer);
		RecommendFactory.showItems(uid, list, false);
		ShowJobItemList("datafile/job/job.csv",list);
	}

	// FilterSalary
	public static double getAverageSalaryByUID(String file, long uid,
			DataModel dataModel) throws IOException, TasteException {
		PreferenceArray pa = dataModel.getPreferencesFromUser(uid);
		// 获取用户薪水
		BufferedReader br = new BufferedReader(new FileReader(new File(file)));
		double avgSal = 0;
		int count = 0;
		Set<Long> st = new HashSet<Long>();// item list
		for (int i = 0; i < pa.length(); i++) {
			st.add(pa.getItemID(i));
		}
		String s = null;
		long itemID;
		while ((s = br.readLine()) != null) {
			String[] cols = s.split(",");
			try {
				itemID = Long.parseLong(cols[0]);
				if (st.contains(itemID)) {
					avgSal += Double.parseDouble(cols[2]);
					count++;
					// 输出用户浏览职位的所有薪水信息
					System.out.println("\tUser:" + uid + "\tviewed item:" + itemID+ "\tSalary:" + Double.parseDouble(cols[2]));
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		br.close();
		return avgSal / count;
	}
	public static void ShowJobItemList(String filename,List<RecommendedItem> list) throws IOException, ParseException
	{
	    // 读取职位信息
		Hashtable<Long,JobItem> ht =LoadJobTable(filename);
		if (list.size() > 0) {
            System.out.printf("Job recommendation Item and Salary list:\n\t");
            for (RecommendedItem recommendation : list) {
                System.out.printf("(%s,%.0f)", recommendation.getItemID(),
                		ht.get(recommendation.getItemID()).getSalary());
            }
            System.out.println();
        }
	}
	public static Set<Long> getFilterSalaryJobID(String file,
			double averageSalary) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(new File(file)));
		Set<Long> jobids = new HashSet<Long>();
		String s = null;
		double avgSalary = 0.8 * averageSalary;
		while ((s = br.readLine()) != null) {
			String[] cols = s.split(",");
			try {
			    //如果80%平均薪水高于浏览职位的工资，则添加到jobids
                //排除掉工资低于平均工资80%的职位
				if (avgSalary > Double.parseDouble(cols[2])) //Note: this is for exclusion list
				{
					jobids.add(Long.parseLong(cols[0]));
					//System.out.println("\tItemID:"+Long.parseLong(cols[0])+"\tSalary:"+Double.parseDouble(cols[2]));
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		br.close();
		return jobids;
	}
	
	public static Hashtable<Long,JobItem> LoadJobTable(String file) throws IOException, ParseException {
		BufferedReader br = new BufferedReader(new FileReader(new File(file)));
		String s = null;
		Hashtable<Long,JobItem> ht =new Hashtable<Long,JobItem>();
		while ((s = br.readLine()) != null) {
			String[] cols = s.split(",");
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			try {
				ht.put(Long.parseLong(cols[0]), new JobItem(Long.parseLong(cols[0]),
						df.parse(cols[1]),Double.parseDouble(cols[2])));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		br.close();
		return ht;
	}
}
class JobItem{
	long itemID;
	Date date;
	double Salary;
	public JobItem(long itemID, Date date, double salary) {
		super();
		this.itemID = itemID;
		this.date = date;
		Salary = salary;
	}
	public long getItemID() {
		return itemID;
	}
	public Date getDate() {
		return date;
	}
	public double getSalary() {
		return Salary;
	}
	public void setItemID(long itemID) {
		this.itemID = itemID;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public void setSalary(double salary) {
		Salary = salary;
	}
}