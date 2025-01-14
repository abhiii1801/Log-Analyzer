# Log Analyzer

## Overview
This project analyzes server log data using PySpark. The script parses log files, processes the data into a structured format, and provides various insights such as traffic, errors, client activity, performance, data usage, and referrer analysis.

## Features
- **Traffic Analysis**: Total requests, breakdown by request types, and requests per API endpoint.
- **Error Analysis**: Error rate and frequency of HTTP status codes.
- **Client Analysis**: Most frequent IP addresses and unique IP count.
- **Performance Metrics**: Response time analysis and APIs with the highest average response time.
- **Data Usage**: Total bytes transferred and average bytes per request.
- **User Agent Analysis**: Most common user agents.
- **Referrer Analysis**: Referrer breakdown and traffic share.

## Output Insights
1. **Traffic Analysis**:
   - Total Requests Made
   - Breakdown of Request Types
   - Count of Requests per API Endpoint

2. **Error Analysis**:
   - Error Rate
   - Frequency of HTTP Status Codes

3. **Client Analysis**:
   - Most Frequent IP Addresses
   - Unique IP Count

4. **Performance Metrics**:
   - Maximum and Average Response Time
   - Slowest APIs by Response Time

5. **Data Usage**:
   - Total and Average Bytes Transferred

6. **User Agent Analysis**:
   - Top 10 User Agents

7. **Referrer Analysis**:
   - Referrer Breakdown by Count and Percentage

## Prerequisites
- Python 3.7+
- Apache Spark 3.x
- Java 8+

