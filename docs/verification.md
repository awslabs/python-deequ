# Verification 

Here are the current supported functionalities of Verification. 

| Class               | Method                                          | Status |
|---------------------|-------------------------------------------------|:------:|
| VerificationResult | status | Done | 
| | checkResults | Done | 
| | metrics | Done | 
| | successMetricsAsDataFrame(spark_session, verificationResult) | Done |
| | successMetricsAsJson(spark_session, verificationResult) | Done | 
| | checkResultsAsDataFrame(spark_session, verificationResult)                               | Done   |
| | checkResultsAsJson(spark_session, verificationResult) | Done | 
| VerificationRunBuilder | VerificationRunBuilder(spark_session, data)                           | Done   |
|  | addCheck(check) | Done |
|  | run() | Done |
|  | useRepository(repository) | Done |
|  | saveOrAppendResult(resultKey) | Done |
| VerificationSuite | VerificationSuite(spark_session) | Done |
|  | onData(data) | Done |

