import pandas as pd
import psycopg2
import logging
from dash import Dash, dcc, html
import plotly.express as px

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_top_customer():
    "Top 5 khách hàng có doanh thu cao nhất"
    query = """
    select 
        c.customerid,
        c.lastname || c.firstname as CustomerFullName,
        p.productname,
        o.totalamount
    from orderdetails o 
    left join customers c on c.customerid = o.customerid
    left join products p on p.productid = o.productid
    order by o.totalamount desc 
    limit 5
    """
    return query

if __name__=='__main__':
    # Kết nối tới PostgreSQL
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost",  
        port=5432
    )

    # Truy vấn dữ liệu
    query = find_top_customer()
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Khởi tạo Dash app
    app = Dash(__name__)

    app.layout = html.Div([
        html.Center("Dashboard")
    ])
    
    fig = px.bar(
        df, x='customerfullname', y='totalamount',
        title='Top 5 khách hàng có doanh thu cao nhất',
        labels={'customerfullname': 'Tên khách hàng', 'totalamount': 'Doanh thu'}
    )
    
    fig.update_traces(text=df['totalamount'], textposition='outside')
    fig.update_layout(uniformtext_minsize=4, uniformtext_mode='hide')
    

    app.layout = html.Div([
        dcc.Graph(figure=fig)
    ])

    app.run(debug=True, port=8050)
    logging.info(f"Visualize data at http://localhost:8050/")
