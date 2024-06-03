import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Dim_Products Amazon Redshift
Dim_ProductsAmazonRedshift_node1716747180666 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-590183810146-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "e_commerce.dim_products", "connectionName": "Redshift connection"}, transformation_ctx="Dim_ProductsAmazonRedshift_node1716747180666")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1716747172956 = glueContext.create_dynamic_frame.from_catalog(database="e_commerce_metadata_db", table_name="s3_input_transactions", transformation_ctx="AWSGlueDataCatalog_node1716747172956")

# Script generated for node Dim_Customers Amazon Redshift
Dim_CustomersAmazonRedshift_node1716747194553 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-590183810146-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "e_commerce.dim_customers", "connectionName": "Redshift connection"}, transformation_ctx="Dim_CustomersAmazonRedshift_node1716747194553")

# Script generated for node Rename Products Schema
RenameProductsSchema_node1716748036293 = ApplyMapping.apply(frame=Dim_ProductsAmazonRedshift_node1716747180666, mappings=[("product_id", "string", "rs_product_id", "string"), ("product_name", "string", "rs_product_name", "string"), ("category", "string", "rs_category", "string"), ("price", "decimal", "rs_price", "decimal"), ("supplier_id", "string", "rs_supplier_id", "string")], transformation_ctx="RenameProductsSchema_node1716748036293")

# Script generated for node Change Schema
ChangeSchema_node1716747846272 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1716747172956, mappings=[("transaction_id", "string", "transaction_id", "string"), ("customer_id", "string", "customer_id", "string"), ("product_id", "string", "product_id", "string"), ("quantity", "long", "quantity", "int"), ("price", "double", "price", "decimal"), ("date", "string", "date", "date"), ("payment_type", "string", "payment_type", "string"), ("status", "string", "status", "string")], transformation_ctx="ChangeSchema_node1716747846272")

# Script generated for node Rename Customer Fields
RenameCustomerFields_node1716748073178 = ApplyMapping.apply(frame=Dim_CustomersAmazonRedshift_node1716747194553, mappings=[("customer_id", "string", "rs_customer_id", "string"), ("first_name", "string", "rs_first_name", "string"), ("last_name", "string", "rs_last_name", "string"), ("email", "string", "rs_email", "string"), ("membership_level", "string", "rs_membership_level", "string")], transformation_ctx="RenameCustomerFields_node1716748073178")

# Script generated for node Transactions Join Products
TransactionsJoinProducts_node1716747972016 = Join.apply(frame1=ChangeSchema_node1716747846272, frame2=RenameProductsSchema_node1716748036293, keys1=["product_id"], keys2=["rs_product_id"], transformation_ctx="TransactionsJoinProducts_node1716747972016")

# Script generated for node Transactions Join Products Join Customers
TransactionsJoinProductsJoinCustomers_node1716748399824 = Join.apply(frame1=TransactionsJoinProducts_node1716747972016, frame2=RenameCustomerFields_node1716748073178, keys1=["customer_id"], keys2=["rs_customer_id"], transformation_ctx="TransactionsJoinProductsJoinCustomers_node1716748399824")

# Script generated for node Change Schema
ChangeSchema_node1716748853615 = ApplyMapping.apply(frame=TransactionsJoinProductsJoinCustomers_node1716748399824, mappings=[("product_id", "string", "product_id", "varchar"), ("price", "decimal", "price", "decimal"), ("date", "date", "transaction_date", "date"), ("rs_last_name", "string", "last_name", "varchar"), ("customer_id", "string", "customer_id", "varchar"), ("rs_supplier_id", "string", "supplier_id", "varchar"), ("payment_type", "string", "payment_type", "varchar"), ("rs_membership_level", "string", "rs_membership_level", "varchar"), ("transaction_id", "string", "transaction_id", "varchar"), ("quantity", "int", "quantity", "int"), ("rs_first_name", "string", "first_name", "varchar"), ("status", "string", "status", "varchar"), ("rs_email", "string", "email", "varchar"), ("rs_category", "string", "category", "varchar"), ("rs_product_name", "string", "product_name", "varchar")], transformation_ctx="ChangeSchema_node1716748853615")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *,
    CASE
        WHEN price < 100 THEN 'Small'
        WHEN price >= 100 AND price < 500 THEN 'Medium'
        WHEN price >= 500 THEN 'Large'
        ELSE 'Unclassified'
    END AS transaction_type
FROM myDataSource;
'''
SQLQuery_node1716750476262 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":ChangeSchema_node1716748853615}, transformation_ctx = "SQLQuery_node1716750476262")

# Script generated for node Amazon Redshift Target
AmazonRedshiftTarget_node1716751484801 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1716750476262, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO e_commerce.fact_transactions USING e_commerce.fact_transactions_temp_eo70e2 ON fact_transactions.transaction_date = fact_transactions_temp_eo70e2.transaction_date AND fact_transactions.status = fact_transactions_temp_eo70e2.status WHEN MATCHED THEN UPDATE SET product_id = fact_transactions_temp_eo70e2.product_id, price = fact_transactions_temp_eo70e2.price, transaction_date = fact_transactions_temp_eo70e2.transaction_date, last_name = fact_transactions_temp_eo70e2.last_name, customer_id = fact_transactions_temp_eo70e2.customer_id, supplier_id = fact_transactions_temp_eo70e2.supplier_id, payment_type = fact_transactions_temp_eo70e2.payment_type, rs_membership_level = fact_transactions_temp_eo70e2.rs_membership_level, transaction_id = fact_transactions_temp_eo70e2.transaction_id, quantity = fact_transactions_temp_eo70e2.quantity, first_name = fact_transactions_temp_eo70e2.first_name, status = fact_transactions_temp_eo70e2.status, email = fact_transactions_temp_eo70e2.email, category = fact_transactions_temp_eo70e2.category, product_name = fact_transactions_temp_eo70e2.product_name, transaction_type = fact_transactions_temp_eo70e2.transaction_type WHEN NOT MATCHED THEN INSERT VALUES (fact_transactions_temp_eo70e2.product_id, fact_transactions_temp_eo70e2.price, fact_transactions_temp_eo70e2.transaction_date, fact_transactions_temp_eo70e2.last_name, fact_transactions_temp_eo70e2.customer_id, fact_transactions_temp_eo70e2.supplier_id, fact_transactions_temp_eo70e2.payment_type, fact_transactions_temp_eo70e2.rs_membership_level, fact_transactions_temp_eo70e2.transaction_id, fact_transactions_temp_eo70e2.quantity, fact_transactions_temp_eo70e2.first_name, fact_transactions_temp_eo70e2.status, fact_transactions_temp_eo70e2.email, fact_transactions_temp_eo70e2.category, fact_transactions_temp_eo70e2.product_name, fact_transactions_temp_eo70e2.transaction_type); DROP TABLE e_commerce.fact_transactions_temp_eo70e2; END;", "redshiftTmpDir": "s3://aws-glue-assets-590183810146-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "e_commerce.fact_transactions_temp_eo70e2", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS e_commerce.fact_transactions (product_id VARCHAR, price DECIMAL, transaction_date DATE, last_name VARCHAR, customer_id VARCHAR, supplier_id VARCHAR, payment_type VARCHAR, rs_membership_level VARCHAR, transaction_id VARCHAR, quantity INTEGER, first_name VARCHAR, status VARCHAR, email VARCHAR, category VARCHAR, product_name VARCHAR, transaction_type VARCHAR); DROP TABLE IF EXISTS e_commerce.fact_transactions_temp_eo70e2; CREATE TABLE e_commerce.fact_transactions_temp_eo70e2 (product_id VARCHAR, price DECIMAL, transaction_date DATE, last_name VARCHAR, customer_id VARCHAR, supplier_id VARCHAR, payment_type VARCHAR, rs_membership_level VARCHAR, transaction_id VARCHAR, quantity INTEGER, first_name VARCHAR, status VARCHAR, email VARCHAR, category VARCHAR, product_name VARCHAR, transaction_type VARCHAR);"}, transformation_ctx="AmazonRedshiftTarget_node1716751484801")

job.commit()