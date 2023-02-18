import yaml 

def csv_to_df(spark,path):
    return spark.read.option("inferSchema", "true").csv(path, header=True)

def output(df,path,write_format):
    df.write.format(write_format).mode('overwrite').option("header", "true").save(path)
    return 

def read_yaml(path):
    with open(path,'r') as f:
        #print("sgsggsgsgsggsgsggsgsgsgsgs")
        #print(yaml.safe_load(f))
        return yaml.safe_load(f)
    
print("Abhinav Anand")