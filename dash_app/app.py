import pandas as pd
import psycopg2
import dash
from dash import dcc, html
import plotly.express as px

# Kết nối tới PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="host.docker.internal",  
    port=5432
)

# Truy vấn dữ liệu
query = "SELECT date, revenue FROM sales_data ORDER BY date"
df = pd.read_sql_query(query, conn)
conn.close()

# Khởi tạo Dash app
app = dash.Dash(__name__)

fig = px.line(df, x='date', y='revenue', title='Revenue Over Time')

app.layout = html.Div([
    html.H1("Doanh thu theo thời gian"),
    dcc.Graph(figure=fig)
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
