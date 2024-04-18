from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# CHANGE BUCKET NAME and FOLDER ACCORDINGLY
S3_DATA_SOURCE_PATH = 's3://spark-project-demo-bucket3/source-data/survey_results_public.csv'
S3_OUTPUT_SOURCE_PATH = 's3://spark-project-demo-bucket3/output-data'


def main():
    # create spark session and read in all data
    spark = SparkSession.builder.appName('ProjectDemoApp').getOrCreate()
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)
    
    # PROBLEM 1: PROCESS DATA FOR CRITERIA - LIVE IN USA AND LEARNING CODE FOR 15+ YEARS
    print('Total number of records in the source data: %s' % all_data.count())
    selected_data = all_data.where((col('Country') == 'United States of America') & (col('YearsCode') > 15))
    print('The number of Stack Overflow users who live in the USA and have been learning code for more than 15 years is: %s' % selected_data.count())
    selected_data.write.mode('overwrite').csv(S3_OUTPUT_SOURCE_PATH)
    print('Selected data was sucessfully saved to s3: %s' % S3_OUTPUT_SOURCE_PATH)
    
    # PROBLEM 2: PROCESS DATA FOR MORE CRITERIA - DEVELOPER BY PROFESSION MAKING 150,000+ USD
    selected_data = all_data.where((col('MainBranch') == 'I am a developer by profession') & 
                                   (col('CompTotal') > '150000') & 
                                   (col('Currency') == 'USD	United States dollar'))
    print('The number of Stack Overflow users that are professional developers and receive over 150000 USD in pay: %s' % selected_data.count())
    selected_data.write.mode('overwrite').csv(S3_OUTPUT_SOURCE_PATH)
    print('Selected data was sucessfully saved to s3: %s' % S3_OUTPUT_SOURCE_PATH)

    # CONTINUATION OF PROBLEM 2 IN ORDER TO FIGURE OUT WHAT PROFESSION MAKES THE MOST MONEY  
    # PROBLEM 3: PROCESS DATA FOR MORE CRITERIA - DEVTYPE BY PROFESSION MAKING OVER 200,000+ USD 
    selected_data = all_data.where((col('CompTotal') > '200000') & 
                                   (col('Currency') == 'USD	United States dollar'))
    print('The number of Stack Overflow users that receive over 200000 USD in pay: %s' % selected_data.count())
    # NOW FILTER THE DATA TO DETERMINE THE DEVTYPE THAT MOST COMMONLY MAKES 200,000+
    filtered_data = all_data.where((col('CompTotal') > '200000') & 
                                   (col('Currency') == 'USD	United States dollar'))
    devtype_counts = filtered_data.groupBy('DevType').count()
    # Order by count descending and collect the result
    ordered_devtypes = devtype_counts.orderBy(col('count').desc()).collect()
    # Extract the first, second, and third most common DevTypes and their counts
    most_common_devtype = ordered_devtypes[0]['DevType']
    most_common_count = ordered_devtypes[0]['count']
    second_common_devtype = ordered_devtypes[1]['DevType'] if len(ordered_devtypes) > 1 else None
    second_common_count = ordered_devtypes[1]['count'] if len(ordered_devtypes) > 1 else None
    third_common_devtype = ordered_devtypes[2]['DevType'] if len(ordered_devtypes) > 2 else None
    third_common_count = ordered_devtypes[2]['count'] if len(ordered_devtypes) > 2 else None
    # Print the results
    print('The total number of people is: %s' % filtered_data.count())
    print('The most common DevType among individuals with a pay range of 200,000+ USD is:', most_common_devtype)
    print('Number of people associated with the most common DevType:', most_common_count)
    print('The second most common DevType:', second_common_devtype)
    print('Number of people associated with the second most common DevType:', second_common_count)
    print('The third most common DevType:', third_common_devtype)
    print('Number of people associated with the third most common DevType:', third_common_count)

    selected_data.write.mode('overwrite').csv(S3_OUTPUT_SOURCE_PATH)
    print('Selected data was sucessfully saved to s3: %s' % S3_OUTPUT_SOURCE_PATH)

if __name__ == '__main__':
    main()
