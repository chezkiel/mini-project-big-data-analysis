import os
import pandas as pd
from pymongo import MongoClient
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "cuaca_db"
COLLECTION_NAME = "data_cuaca"

def load_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    data = list(collection.find({}))
    df = pd.DataFrame(data)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Makassar")

        df["temperature"] = df["temperature"].round().astype(int)
    return df


app = dash.Dash(__name__)
app.title = "Dashboard Cuaca Interaktif"

df = load_data()
nama_kota = df["city"].iloc[0] if not df.empty else "Tidak diketahui"
last_update = df["timestamp"].max() if not df.empty else None

app.layout = html.Div([
    html.H1(f"Dashboard Cuaca - Kota: {nama_kota}"),
    html.P(id='last-update-text', children=(
        f"Data terakhir diupdate pada: {last_update.strftime('%Y-%m-%d %H:%M:%S')}"
        if last_update else "Data tidak tersedia"
    )),
    dcc.DatePickerRange(
        id='date-picker-range',
        start_date=df["timestamp"].min(),
        end_date=df["timestamp"].max()
    ),
    html.Div(id='summary-stats', style={'padding': '20px', 'border': '1px solid #ddd', 'margin-bottom': '20px'}),
    dcc.Graph(id='line-temp'),
    dcc.Graph(id='line-hum'),
    dcc.Graph(id='bar-weather-freq'),
    dcc.Graph(id='hist-temp'),
    dcc.Graph(id='hist-hum'),

    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,
        n_intervals=0
    )
])

@app.callback(
    [Output('summary-stats', 'children'),
     Output('line-temp', 'figure'),
     Output('line-hum', 'figure'),
     Output('bar-weather-freq', 'figure'),
     Output('hist-temp', 'figure'),
     Output('hist-hum', 'figure'),
     Output('last-update-text', 'children'),
     Output('date-picker-range', 'start_date'),
     Output('date-picker-range', 'end_date')],
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('interval-component', 'n_intervals')]
)
def update_graph(start_date, end_date, n):
    df = load_data()
    if df.empty:
        return html.P("Data tidak tersedia."), {}, {}, {}, {}, {}, "Data tidak tersedia", None, None

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    filtered_df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)]

    # Statistik deskriptif
    avg_temp = filtered_df["temperature"].mean()
    min_temp = filtered_df["temperature"].min()
    max_temp = filtered_df["temperature"].max()
    std_temp = filtered_df["temperature"].std()

    avg_hum = filtered_df["humidity"].mean()
    min_hum = filtered_df["humidity"].min()
    max_hum = filtered_df["humidity"].max()
    std_hum = filtered_df["humidity"].std()

    summary = html.Div([
        html.H3("Statistik Deskriptif"),
        html.P(f"Suhu (°C): Rata-rata = {avg_temp:.2f}, Minimum = {min_temp:.2f}, Maksimum = {max_temp:.2f}, Std Dev = {std_temp:.2f}"),
        html.P(f"Kelembaban (%): Rata-rata = {avg_hum:.2f}, Minimum = {min_hum:.2f}, Maksimum = {max_hum:.2f}, Std Dev = {std_hum:.2f}"),
    ])

    fig_temp = px.line(filtered_df, x="timestamp", y="temperature", title="Suhu (°C) dari Waktu ke Waktu")
    fig_temp.update_layout(yaxis_title='Suhu (°C)')

    fig_hum = px.line(filtered_df, x="timestamp", y="humidity", title="Kelembaban (%) dari Waktu ke Waktu")
    fig_hum.update_layout(yaxis_title='Kelembaban (%)')

    weather_counts = filtered_df['weather'].value_counts().reset_index()
    weather_counts.columns = ['Cuaca', 'Frekuensi']
    fig_weather_freq = px.bar(weather_counts, x='Cuaca', y='Frekuensi', title='Frekuensi Jenis Cuaca')

    fig_hist_temp = px.histogram(filtered_df, x="temperature", nbins=30, title="Distribusi Suhu")
    fig_hist_hum = px.histogram(filtered_df, x="humidity", nbins=30, title="Distribusi Kelembaban")

    last_update = df["timestamp"].max().strftime('%Y-%m-%d %H:%M:%S')

    return summary, fig_temp, fig_hum, fig_weather_freq, fig_hist_temp, fig_hist_hum, \
        f"Data terakhir diupdate pada: {last_update}", df["timestamp"].min(), df["timestamp"].max()

if __name__ == "__main__":
    app.run(debug=True)