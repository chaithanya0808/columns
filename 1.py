from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def underscore_to_camelcase(column_name):
    parts = column_name.split('_')
    return parts[0] + ''.join(x.capitalize() for x in parts[1:])

def convert_all_columns_to_camelcase(df):
    new_columns = [underscore_to_camelcase(column) for column in df.columns]
    df_camelcase = df
    for old_col, new_col in zip(df.columns, new_columns):
        df_camelcase = df_camelcase.withColumnRenamed(old_col, new_col)
    return df_camelcase

# Example usage
if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("underscore_to_camelcase").getOrCreate()

    # Create a sample DataFrame
    data = [("john_doe", 25, "male"), ("jane_smith", 30, "female")]
    columns = ["user_name", "age", "gender"]
    df = spark.createDataFrame(data, columns)

    # Display the original DataFrame
    print("Original DataFrame:")
    df.show()

    # Convert all columns to camelCase
    df_camelcase = convert_all_columns_to_camelcase(df)

    # Display the DataFrame with camelCase columns
    print("DataFrame with camelCase columns:")
    df_camelcase.show()

    # Stop the Spark session
    spark.stop()
