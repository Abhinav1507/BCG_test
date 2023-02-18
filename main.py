from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
import os
import sys
import df_load
#SPARK_HOME='/home/zied/.local/lib/python3.8/site-packages/pyspark'
#PYSPARK_PYTHON=python3.10
#ys.setenv(SPARK_HOME = "") 
#os.environ["PYSPARK_PYTHON"] = r"C:/Users/DELL/AppData/Local/Programs/Python/Python310/python.exe"
#os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#df_load.read_yaml

class US_veichal_accident_analysis:
        def __init__(self,config_file_path):
                data_paths = df_load.read_yaml(config_file_path).get("INPUT_FILENAME")
                
                self.charges = df_load.csv_to_df(spark, data_paths.get("Charges"))
                self.damages = df_load.csv_to_df(spark, data_paths.get("Damages"))
                self.endorse = df_load.csv_to_df(spark, data_paths.get("Endorse"))
                self.primary_person = df_load.csv_to_df(spark, data_paths.get("Primary_Person"))
                self.units = df_load.csv_to_df(spark, data_paths.get("Units"))
                self.restrict = df_load.csv_to_df(spark, data_paths.get("Restrict"))
        def Total_male_accident(self,output_path,output_format):
                df=self.primary_person.filter(self.primary_person.PRSN_GNDR_ID =='MALE').filter(self.primary_person.DEATH_CNT>0)
                df_load.output(df,output_path,output_format)
                return df.count()
        def TwoWheeler_crash(self,output_path,output_format):
                df=self.units.filter(self.units.VEH_BODY_STYL_ID.contains('MOTORCYCLE'))
                df_load.output(df,output_path,output_format)
                return df.count()
        def State_with_highest_female_accident(self,output_path,output_format):
                df = self.primary_person.filter(self.primary_person.PRSN_GNDR_ID == "FEMALE"). \
                groupby("DRVR_LIC_STATE_ID").count(). \
                orderBy(col("count").desc())
                df_load.output(df,output_path,output_format)
                return df.first().DRVR_LIC_STATE_ID
        
        def get_top_vehicle_contributing_to_injuries(self,output_path,output_format):
                df = self.units.filter(self.units.VEH_MAKE_ID != "NA"). \
            withColumn('TOT_CASUALTIES_CNT', self.units[35] + self.units[36]). \
            groupby("VEH_MAKE_ID").sum("TOT_CASUALTIES_CNT"). \
            withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG"). \
            orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
                df_top_5_to_15 = df.limit(15).subtract(df.limit(5))
                df_load.output(df,output_path,output_format)
                return [veh[0] for veh in df_top_5_to_15.select("VEH_MAKE_ID").collect()]
        
        def get_top_ethnic_ug_crash_for_each_body_style(self, output_path, output_format):
                w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())

                df = self.units.join(self.primary_person, on=['CRASH_ID'], how='inner'). \
                filter(~self.units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                                "OTHER  (EXPLAIN IN NARRATIVE)"])). \
                filter(~self.primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])). \
                groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
                withColumn("row", row_number().over(w)).filter(col("row") == 1).drop("row", "count")

                df_load.output(df,output_path,output_format)

                df.show(truncate=False)

        def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(self, output_path, output_format):
                df = self.units.join(self.primary_person, on=['CRASH_ID'], how='inner'). \
                dropna(subset=["DRVR_ZIP"]). \
                filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
                groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
                
                df_load.output(df,output_path,output_format)
                return [row[0] for row in df.collect()]
        
        def get_crash_ids_with_no_damage(self, output_path, output_format):

                df = self.damages.join(self.units, on=["CRASH_ID"], how='inner'). \
            filter(
            ( (self.units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") & (~self.units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ) | ( (self.units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") & (~self.units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ) ). filter(self.damages.DAMAGED_PROPERTY == "NONE"). filter(self.units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")

                df_load.output(df,output_path,output_format)

                return [row[0] for row in df.collect()]
        
        def get_top_5_vehicle_brand(self, output_path, output_format):

                top_25_state_list = [row[0] for row in self.units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
            groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
                
                top_10_used_vehicle_colors = [row[0] for row in self.units.filter(self.df_units.VEH_COLOR_ID != "NA").
            groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]
                
                df = self.charges.join(self.primary_person, on=['CRASH_ID'], how='inner'). \
                        join(self.units, on=['CRASH_ID'], how='inner'). \
                        filter(self.charges.CHARGE.contains("SPEED")). \
                        filter(self.primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
                        filter(self.units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
                        filter(self.units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
                        groupby("VEH_MAKE_ID").count(). \
                        orderBy(col("count").desc()).limit(5)
                        
                df_load.output(df,output_path,output_format)
                return [row[0] for row in df.collect()]
                        
if __name__ == '__main__':
        spark = SparkSession \
        .builder \
        .appName("USVehicleAccidentAnalysis") \
        .getOrCreate()

        config_file_path = "config_location.yaml"

        spark.sparkContext.setLogLevel("ERROR")
        output_paths = df_load.read_yaml(config_file_path).get("OUTPUT_PATH")
        
        print(output_paths)

        UA=US_veichal_accident_analysis(config_file_path)

        output_paths = df_load.read_yaml(config_file_path).get("OUTPUT_PATH")
        output_format = df_load.read_yaml(config_file_path).get("FILE_FORMAT")

        print("1. Result:", UA.Total_male_accident(output_paths.get(1), output_format.get("Output")))

        print("2. Result:", UA.TwoWheeler_crash(output_paths.get(2), output_format.get("Output")))

        print("3. Result:", UA.State_with_highest_female_accident(output_paths.get(3), output_format.get("Output")))

        print("4. Result:", UA.get_top_vehicle_contributing_to_injuries(output_paths.get(4),output_format.get("Output")))

        print("5. Result:", UA.get_top_ethnic_ug_crash_for_each_body_style(output_paths.get(5), output_format.get("Output")))

        print("6. Result:", UA.get_top_5_zip_codes_with_alcohols_as_cf_for_crash(output_paths.get(6),output_format.get("Output")))

        print("7. Result:", UA.get_crash_ids_with_no_damage(output_paths.get(7), output_format.get("Output")))

        print("8. Result:", UA.get_top_5_vehicle_brand(output_paths.get(8), output_format.get("Output")))

        print('Done')

        spark.stop()