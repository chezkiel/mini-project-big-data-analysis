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

PULAU_KOTA = {
    "Sumatera": ["Medan", "Padang", "Palembang"],
    "Jawa": ["Jakarta", "Bandung", "Surabaya"],
    "Kalimantan": ["Pontianak", "Banjarmasin", "Samarinda"],
    "Sulawesi": ["Makassar", "Manado", "Palu"],
    "Papua": ["Jayapura"],
    "Bali–Nusa": ["Denpasar", "Mataram"],
    "Maluku": ["Ambon"]
}

def get_pulau(city):
    for pulau, kota_list in PULAU_KOTA.items():
        if city in kota_list:
            return pulau
    return "Lainnya"

def load_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    data = list(collection.find({}))
    df = pd.DataFrame(data)

    if not df.empty:
        if "local_time" in df.columns:
            df["timestamp"] = pd.to_datetime(df["local_time"])
        else:
            df["timestamp"] = pd.to_datetime("now")

        df["temperature"] = df["temperature"].astype(float).round()
        df["humidity"] = df["humidity"].astype(int)
        df["pulau"] = df["city"].apply(get_pulau)
    
    return df

app = dash.Dash(__name__)
app.title = "Dashboard Cuaca Indonesia"

df = load_data()
start_date = df["timestamp"].min() if not df.empty else None
end_date = df["timestamp"].max() if not df.empty else None
available_pulau = sorted(df["pulau"].unique()) if not df.empty else []

app.layout = html.Div([
    html.H1("Dashboard Cuaca Regional Indonesia"),
    html.P(id='last-update-text'),
    dcc.DatePickerRange(
        id='date-picker-range',
        start_date=start_date,
        end_date=end_date
    ),
    dcc.Dropdown(
        id="pulau-dropdown",
        options=[{"label": p, "value": p} for p in available_pulau],
        value=available_pulau,
        multi=True,
        placeholder="Pilih pulau"
    ),
    html.Div(id='summary-stats', style={'padding': '20px', 'border': '1px solid #ccc', 'margin-bottom': '20px'}),
    dcc.Graph(id='line-temp'),
    dcc.Graph(id='line-hum'),
    dcc.Graph(id='bar-pulau-avg-temp'),
    dcc.Graph(id='bar-weather-freq'),
    dcc.Graph(id='heatmap-temp'),

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
     Output('bar-pulau-avg-temp', 'figure'),
     Output('bar-weather-freq', 'figure'),
     Output('heatmap-temp', 'figure'),
     Output('last-update-text', 'children')],
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('pulau-dropdown', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_graphs(start_date, end_date, selected_pulau, n):
    df = load_data()
    if df.empty:
        return html.P("Data tidak tersedia."), {}, {}, {}, {}, {}, "Data tidak tersedia"

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    if not selected_pulau or len(selected_pulau) == 0:
        filtered_df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)]
    else:
        filtered_df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date) & (df["pulau"].isin(selected_pulau))]

    if filtered_df.empty:
        return html.P("Tidak ada data untuk filter tersebut."), {}, {}, {}, {}, {}, ""

    # Statistik deskriptif suhu & kelembaban (gabungan)
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
        html.P(f"Suhu (°C): Rata-rata = {avg_temp:.2f}, Min = {min_temp:.2f}, Max = {max_temp:.2f}, Std Dev = {std_temp:.2f}"),
        html.P(f"Kelembaban (%): Rata-rata = {avg_hum:.2f}, Min = {min_hum:.2f}, Max = {max_hum:.2f}, Std Dev = {std_hum:.2f}")
    ])

    # Grafik Line per pulau (pakai facet_row berdasarkan pulau)
    fig_temp = px.line(filtered_df, x="timestamp", y="temperature", color="city", facet_row="pulau",
                       title="Suhu dari Waktu ke Waktu per Pulau",
                       height=300 * len(selected_pulau) if selected_pulau else 600)
    fig_temp.update_layout(margin=dict(t=50, b=50))

    fig_hum = px.line(filtered_df, x="timestamp", y="humidity", color="city", facet_row="pulau",
                      title="Kelembaban dari Waktu ke Waktu per Pulau",
                      height=300 * len(selected_pulau) if selected_pulau else 600)
    fig_hum.update_layout(margin=dict(t=50, b=50))

    # Bar frekuensi jenis cuaca (filter per pulau)
    weather_counts = filtered_df["weather"].value_counts().reset_index()
    weather_counts.columns = ["weather", "count"]
    fig_weather = px.bar(weather_counts, x="weather", y="count", title="Frekuensi Jenis Cuaca")

    # Bar rata-rata suhu per pulau (dari seluruh data tanpa filter)
    avg_temp_pulau_all = df.groupby("pulau")["temperature"].mean().reset_index()
    fig_pulau_temp = px.bar(avg_temp_pulau_all, x="pulau", y="temperature", title="Rata-rata Suhu per Pulau (Seluruh Data)")

    # Heatmap suhu kota per waktu (filter per pulau)
    heatmap_df = filtered_df.pivot_table(index="city", columns="timestamp", values="temperature")
    fig_heatmap = px.imshow(heatmap_df, aspect="auto", labels=dict(color="Suhu (°C)"), title="Heatmap Suhu per Kota")
    
    last_update = filtered_df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S")

    return summary, fig_temp, fig_hum, fig_weather, fig_pulau_temp, fig_heatmap, f"Data terakhir diupdate pada: {last_update}"


if __name__ == "__main__":
    app.run(debug=True)