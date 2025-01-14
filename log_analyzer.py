from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("LogAnalyzer").getOrCreate()

def mapping_logs(log_input):
    try:
        splitted = log_input.split('"')

        ip_add_str = splitted[0].split()
        ip = ip_add_str[0]
        log_name = ip_add_str[1]
        user_id = ip_add_str[2]
        date_time = f"{ip_add_str[3][1:]} {ip_add_str[4][:-1]}"

        req_type_str = splitted[1].split()
        req_type = req_type_str[0]
        api = req_type_str[1]
        protocol = req_type_str[2]

        stat_str = splitted[2].split()
        status_code = int(stat_str[0])
        byte = int(stat_str[1]) if stat_str[1].isdigit() else 0

        referrer = splitted[3].strip()
        ua_str = splitted[5].strip()
        res_time = int(splitted[6].strip())

        return Row(ip_addr=ip, log_name=log_name, user_id=user_id, date_time=date_time, req_type=req_type, api=api, protocol=protocol, status_code=status_code, byte=byte, referrer=referrer, ua_str = ua_str ,res_time=res_time)
    except Exception as e:
        print(e)
        return None 

input_logs = spark.sparkContext.textFile("logfiles_short.log")
input_logs_mapped = input_logs.map(mapping_logs).filter(lambda x: x is not None)

logs_df = spark.createDataFrame(input_logs_mapped)
logs_df.cache()
# logs_df.show(100)

print("Traffic Insights")
req_count = logs_df.count()
print(f"Total Requests Made: {req_count}")

print("Breakdown of request types: ")
grouped_by_req_type = logs_df.groupBy("req_type").agg(func.count("ip_addr").alias("request_type_count"),(func.count("ip_addr") / req_count * 100).alias("percentage")).sort(func.desc("request_type_count"))
grouped_by_req_type.show()

print("Count of requests per API endpoint: ")
grouped_by_api = logs_df.groupBy("api").agg(func.count("ip_addr").alias("request_api_count")).sort(func.desc("request_api_count"))
grouped_by_api.show()

print("Error Analysis")

error_requests = logs_df.filter(logs_df.status_code >= 400).count()
error_rate = (error_requests / req_count) * 100
print(f"Error Rate: {error_rate:.2f}%")

print("Frequency of status codes sorted by most to least: ")
grouped_by_error_codes = logs_df.groupBy("status_code").agg(func.count("ip_addr").alias("codes_count")).sort(func.desc("codes_count"))
grouped_by_error_codes.show()

print("Client Analysis")
print("Most frequent IP addresses")
grouped_by_ip_addr = logs_df.groupBy("ip_addr").agg(func.count("ip_addr").alias("ip_visited_count")).sort(func.desc("ip_visited_count")).limit(10)
grouped_by_ip_addr.show()

unique_ips_count = logs_df.select("ip_addr").distinct().count()
print(f"Unique IP Addresses: {unique_ips_count}")

print("Performance Metrics: ")
print("IP with Maximum Response Time")
max_res_time = logs_df.select("ip_addr","date_time","res_time").agg(func.max("res_time"))
max_res_time = max_res_time.collect()
max_res_time = max_res_time[0][0]
print(f"Maximum Response Time: {max_res_time}")
max_res_time_row = logs_df.filter(logs_df.res_time == max_res_time).select("ip_addr", "date_time", "res_time").collect()[0]
print(f"IP Address: {max_res_time_row['ip_addr']}, DateTime: {max_res_time_row['date_time']}")

avg_res_time = logs_df.select("ip_addr","date_time","res_time").agg(func.round(func.avg("res_time"),2))
avg_res_time = avg_res_time.collect()
avg_res_time = avg_res_time[0][0]
print(f"Average Response Time: {avg_res_time}")

print("APIs with Response Time")
slowest_apis = logs_df.groupBy("api").agg(func.round(func.avg("res_time"),2).alias("avg_res_time")).sort(func.desc("avg_res_time"))
slowest_apis.show()

print("Data Usage")
total_bytes = logs_df.agg(func.sum("byte")).collect()[0][0]
print(f"Total bytes transferred: {total_bytes}")

avg_bytes_per_request = logs_df.agg(func.round(func.avg("byte"), 2)).collect()[0][0]
print(f"Average bytes per request: {avg_bytes_per_request}")

print("User Agent Analysis")
top_user_agents = logs_df.groupBy("ua_str").agg(func.count("ua_str").alias("ua_count")).sort(func.desc("ua_count")).limit(10)
top_user_agents.show()

print("Referrer Analysis")

referrer_analysis = logs_df.groupBy("referrer").agg(
    func.count("referrer").alias("referrer_count"),
    (func.count("referrer") / req_count * 100).alias("percentage")
).sort(func.desc("referrer_count"))
referrer_analysis.show()

spark.stop()