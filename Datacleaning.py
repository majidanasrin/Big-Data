from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, trim, lower, when

spark = SparkSession.builder.appName("StudentDataCleaning").getOrCreate()

student_records = [
    ("Anjali Nair", 85, "Maths", "A"),
    ("anjali nair", 85, "Maths", "A"),
    ("Rahul Menon", None, "Physics", "C"),
    ("RAHUL MENON", 72, "PHYSICS", "C"),
    (None, 90, "Chemistry", "A"),
    ("Neha Pillai", 95, " Maths ", " A "),
    ("  Arjun Varma  ", 78, "Biology", "B"),
    ("Arjun Varma", 78, "BIOLOGY", "B"),
    ("Sneha Krishnan", 40, "Maths", "F"),
    ("Akhil Das", 92, "Chemistry", "unknown"),
    ("Revathi S", -1, "Physics", "D"),
    ("Ravi", 88, None, "B"),
    ("Meera Mohan", 150, "Biology", "A"),
    ("meera mohan", 150, "BIOLOGY", "A"),
]

columns = ["StudentName", "Score", "Subject", "Grade"]

df = spark.createDataFrame(student_records, columns)
print("Original Data:")
df.show()

# Fill missing values
df_filled = df.fillna({
    "StudentName": "Unknown Student",
    "Score": 0,
    "Subject": "General",
    "Grade": "N/A"
})
print("After Filling Nulls:")
df_filled.show()

# Drop records with missing StudentName or Subject
df_dropped = df.dropna(subset=["StudentName", "Subject"])  
print("After Dropping Critical Nulls:")
df_dropped.show()

# Clean text fields
df_cleaned = df_dropped \
    .withColumn("StudentName", trim(lower(col("StudentName")))) \
    .withColumn("Subject", trim(lower(col("Subject")))) \
    .withColumn("Grade", trim(col("Grade")))

print("After Text Cleaning (trim + lower):")
df_cleaned.show()

# Remove duplicates
df_dedup = df_cleaned.dropDuplicates()
print("After Removing Duplicates:")
df_dedup.show()

# Calculate average score
avg_score = df_dedup.select(avg("Score")).first()[0]
print(f"Average Score: {avg_score:.2f}")

# Detect outliers
outliers = df_dedup.filter((col("Score") > 100) | (col("Score") < 0))
print("Outliers Detected (Invalid Scores):")
outliers.show()

# Replace outliers with average score
df_final = df_dedup.withColumn(
    "Score",
    when((col("Score") > 100) | (col("Score") < 0), avg_score).otherwise(col("Score"))
)

print("Final Data (Outliers Fixed with Average):")
df_final.show()
