from flask import Flask, render_template
import pandas as pd
import psycopg2
import os

app = Flask(__name__)

conn_string = f"dbname={os.environ.get("db")} user={os.environ.get("user")} host={os.environ.get("host")} port={os.environ.get("port")} password={os.environ.get("password")}"

def connect_to_db():
    conn = psycopg2.connect(conn_string)
    return conn

def calculate_weekly_gain(df):
    weekly_closing_prices = df.groupby(['Week', 'Symbol'])['ClosePrice'].agg(['first', 'last'])
    weekly_closing_prices['WeeklyGain'] = (weekly_closing_prices['last'] - weekly_closing_prices['first']) / weekly_closing_prices['first'] * 100
    return weekly_closing_prices

def map_data_by_week(df):
    df['Max_Delivery'] = df['%DlyQttoTradedQty']
    df['Mean_Delivery'] = df['%DlyQttoTradedQty']
    df['Date'] = pd.to_datetime(df['Date'])
    df['Week'] = df['Date'].dt.to_period('W')

    weekly_data = df.groupby(['Week', 'Symbol']).agg({
        'TotalTradedQuantity': 'mean',
        'DeliverableQty': 'mean',
        '%DlyQttoTradedQty': 'mean',
        'Max_Delivery': 'max'

    }).reset_index()
    weekly_gains = calculate_weekly_gain(df)
    print(weekly_gains)
    weekly_data = pd.merge(weekly_data, weekly_gains, on=['Week', 'Symbol'], how='left')
    return weekly_data



@app.route('/')
def home():   

    query = """
        SELECT *
        FROM marketdata
        WHERE (CAST("DeliverableQty" AS INT)) > (
                SELECT AVG(CAST("DeliverableQty" AS INT))
                FROM marketdata
                WHERE CAST("Date" AS DATE) >= CURRENT_DATE - INTERVAL '1 month'::interval
            )
        AND (CAST("DeliverableQty" AS INT)) > (
                SELECT AVG(CAST("DeliverableQty" AS INT))
                FROM marketdata
                WHERE CAST("Date" AS DATE) >= CURRENT_DATE - INTERVAL '1 week'::interval
            )
        AND "SMA5" > "SMA20"
        ORDER BY "daily_change" DESC;
    """
   
    conn = psycopg2.connect(conn_string)
    df = pd.read_sql_query(query, conn)
    df = df[['Company Name', 'Industry', 'Date','Symbol', 'ClosePrice','TotalTradedQuantity', 'DeliverableQty','%DlyQttoTradedQty', 'daily_change', 'SMA5', 'SMA20']]

    numeric_columns = ['%DlyQttoTradedQty', 'daily_change', 'ClosePrice','SMA5', 'SMA20']
    df[numeric_columns] = df[numeric_columns].astype(float)

    df_rounded = df.round({'%DlyQttoTradedQty': 2, 'daily_change': 2, 'SMA5': 2, 'SMA20': 2})
    print(df_rounded)
   
    weekly_data = map_data_by_week(df)

    weeks = sorted(weekly_data['Week'].unique())

        
    filtered_data1 = weekly_data[(weekly_data['Week'] == weeks[-1]) & (weekly_data['%DlyQttoTradedQty'] > 50)]
    filtered_data1 = filtered_data1[filtered_data1['WeeklyGain'] > 0]
    print(filtered_data1)

    filtered_data2 = weekly_data[(weekly_data['Week'] == weeks[-2]) & (weekly_data['%DlyQttoTradedQty'] > 50)]
    filtered_data2 = filtered_data2[filtered_data2['WeeklyGain'] > 0]
    print(filtered_data1)


    df_rounded['Date'] = pd.to_datetime(df['Date'])
    last_date = df['Date'].tail(1).values[0]
    formatted_date = str(last_date).split('T')[0]
  
    filtered_data = df_rounded[df_rounded['Date'] == formatted_date]
    
    table_html1= filtered_data.to_html(classes='GeneratedTable',index=False)
    table_html2= filtered_data1.to_html(classes='GeneratedTable',index=False)
    table_html3= filtered_data2.to_html(classes='GeneratedTable',index=False)
  
    return render_template('stocks_insights.html', table1=table_html1,table2=table_html2,table3=table_html3)

