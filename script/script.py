import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Database Connection Setup
def get_db_connection():
    engine = create_engine('postgresql+psycopg2://username:password@localhost:5432/chipchip')
    return engine

# 1. Cohort Analysis

def cohort_analysis(engine):
    query = """
    SELECT 
        DATE_TRUNC('month', u.created_at) AS cohort_month,
        DATE_TRUNC('month', g.created_at) AS participation_month,
        COUNT(DISTINCT u.user_id) AS users
    FROM 
        "user" u
    JOIN 
        "group_cart" gc ON u.user_id = gc.user_id
    JOIN 
        "group" g ON gc.group_id = g.group_id
    WHERE 
        g.created_at >= u.created_at
    GROUP BY 
        cohort_month, participation_month
    ORDER BY 
        cohort_month, participation_month;
    """
    
    df = pd.read_sql(query, engine)

    # Pivot table for cohort retention
    cohort_pivot = df.pivot_table(
        index='cohort_month', 
        columns='participation_month', 
        values='users', 
        aggfunc=np.sum
    )

    # Calculate retention percentages
    cohort_retention = cohort_pivot.divide(cohort_pivot.iloc[:, 0], axis=0)

    return cohort_retention

# 2. Dynamic Popular Categories and Sales Growth

def calculate_sales_growth(engine):
    query = """
    WITH category_sales AS (
        SELECT 
            DATE_TRUNC('month', o.created_at) AS sales_month,
            c.category_id,
            SUM(o.amount) AS total_sales
        FROM 
            "order" o
        JOIN 
            "group" g ON o.group_id = g.group_id
        JOIN 
            products p ON g.product_id = p.product_id
        JOIN 
            category c ON p.category_id = c.category_id
        WHERE 
            o.status = 'completed'
        GROUP BY 
            sales_month, c.category_id
    )
    SELECT 
        current_month.category_id,
        current_month.sales_month AS current_month,
        current_month.total_sales AS current_sales,
        previous_month.total_sales AS previous_sales,
        ((current_month.total_sales - previous_month.total_sales) / NULLIF(previous_month.total_sales, 0)) * 100 AS growth_percentage
    FROM 
        category_sales current_month
    LEFT JOIN 
        category_sales previous_month
        ON current_month.category_id = previous_month.category_id
        AND current_month.sales_month = previous_month.sales_month + INTERVAL '1 month';
    """

    df = pd.read_sql(query, engine)
    return df

# 3. Reusable Data Preprocessing Class

class DataPreprocessor:
    def __init__(self, engine):
        self.engine = engine

    def preprocess_table(self, table_name):
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, self.engine)

        # Handle NULLs
        df.fillna(0, inplace=True)

        # Encode categorical variables
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype('category').cat.codes

        # Aggregate timestamps
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at']).dt.to_period('M')

        return df

# Usage Example
if __name__ == "__main__":
    engine = get_db_connection()

    # Cohort Analysis
    retention_df = cohort_analysis(engine)
    print("Cohort Retention Analysis:\n", retention_df)

    # Sales Growth
    growth_df = calculate_sales_growth(engine)
    print("Sales Growth Analysis:\n", growth_df)

    # Preprocess Data
    preprocessor = DataPreprocessor(engine)
    user_df = preprocessor.preprocess_table('user')
    print("Preprocessed User Data:\n", user_df.head())

