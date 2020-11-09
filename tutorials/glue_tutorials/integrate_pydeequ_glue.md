# How to: Integrate PyDeequ with AWS Glue 


## 1. Obtain and Store the Packages: 

- [Deequ package](https://mvnrepository.com/artifact/com.amazon.deequ/deequ/1.0.3)
     - Download the 1.0.3 package. 
     
- PyDeequ: 
     - Compress and get the .zip PyDeequ package. The package should be at the root of the archive and must contain the __init__.py file. 
     
- Create an S3 bucket. Upload and store the Deequ and PyDeequ package to the S3 bucket.
 
 For more information about incorporating packages with Glue: 
 https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html

## 2. Set up IAM Permissions for Glue:
 - Follow steps 1-7 for setting an [IAM permission for AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html).

## 3. Set up your PyDeequ Endpoint in the Console: 

 - Follow the tutorial [Adding a Developmental Endpoint](https://docs.aws.amazon.com/glue/latest/dg/add-dev-endpoint.html) to create an endpoint for debugging, editing and running Python Scripts within Glue.
     - However, modify these fields to include the following: 
         - Python Library Path: include the S3 path to the 'pydeequ.zip' library
         - Dependant Jars Path: include the S3 path to the  deequ library

## 4. Use an Amazon SageMaker Notebook with your Development Endpoint: 
   - Follow the [Tutorial: Use an Amazon SageMaker Notebook with Your Development Endpoint](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-sage.html)

### You should now be able to import pydeequ from within this notebook! 