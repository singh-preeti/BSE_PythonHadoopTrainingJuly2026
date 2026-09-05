import csv
import math
from pathlib import Path


# CSV file location
DATA_FILE = Path(__file__).parent / "transactions.csv"


# --------------------------------------------------
# Read one column from CSV
# --------------------------------------------------
def read_column(column_name):

    values = []

    with open(DATA_FILE, "r") as file:

        reader = csv.DictReader(file)

        for row in reader:
            values.append(float(row[column_name]))

    return values


# --------------------------------------------------
# Calculate skewness
# --------------------------------------------------
def skewness(values):

    average = sum(values) / len(values)

    # Standard deviation
    sd = math.sqrt(
        sum((x - average) ** 2 for x in values) / len(values)
    )

    if sd == 0:
        return 0
    # values = [10,20,30,40,50,100]
    # Skewness formula
    skew = sum(
        #(10-20) = ( -10) 3  = -1000
        # (20 -20) = 0 = 0
        #(30-20) = +10 = 1000
        (x - average) ** 3 for x in values
        # sum(x-avg)3
    ) / len(values) / sd ** 3

    return skew


# --------------------------------------------------
# Tell us what the skewness means
# --------------------------------------------------
def check_skewness(value):

    if value > 0.5:
        return "Right skewed"

    elif value < -0.5:
        return "Left skewed"

    else:
        return "Approximately balanced"


# --------------------------------------------------
# Simple IQR clipping
# --------------------------------------------------
def iqr_clip(values):

    values = sorted(values)
    # values = [10,12,13,15,16,18,20,100]

    q1 = values[len(values) // 4]
     # 8 // 4 = 2
     # q1 = values[2]  = 13
    q3 = values[(3 * len(values)) // 4]
      # q3 = values[6] =20

    iqr = q3 - q1
     # 20-13 = 7
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    clipped = []

    for value in values:

        if value < lower:
            value = lower

        if value > upper:
            value = upper

        clipped.append(value)

    return clipped


# --------------------------------------------------
# Simple log transformation
# --------------------------------------------------
def log_transform(values):

    return [math.log1p(x) for x in values]

   # [ 0,1,2,3,9] log(1+x)
# --------------------------------------------------
# Analyze one column
# --------------------------------------------------
def analyze(column_name):

    values = read_column(column_name)

    original = skewness(values)

    clipped_values = iqr_clip(values)
    clipped = skewness(clipped_values)

    logged_values = log_transform(values)
    logged = skewness(logged_values)

    print("\nColumn:", column_name)

    print("Original skewness :", round(original, 3))
    print("Original type     :", check_skewness(original))

    print("After IQR clipping:", round(clipped, 3))

    print("After log transform:", round(logged, 3))


# --------------------------------------------------
# Main program
# --------------------------------------------------
def main():

    print("TRANSACTION DATA SKEWNESS")

    analyze("price")

    analyze("quantity")


# Start program
if __name__ == "__main__":
    main()
