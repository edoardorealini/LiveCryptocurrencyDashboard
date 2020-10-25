import plotly.graph_objects as go

import dash
import dash_html_components as html
import dash_core_components as dcc

from dash.dependencies import Input, Output

from cassandra.cqlengine import columns
from cassandra import ConsistencyLevel
from cassandra.cqlengine.models import Model
from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table
from cassandra.query import SimpleStatement

import pandas as pd
from datetime import datetime

cluster = Cluster(["127.0.0.1"], port=9042)
session = cluster.connect('cryptos_keyspace', wait_for_all_pools=True)
session.execute("USE cryptos_keyspace")

cryptos = ["bitcoin", "ethereum", "tether", "xrp",
           "litecoin", "cardano", "iota", "eos", "stellar"]

days = [30, 45, 60, 90, 180, 365]

crypto_colors = {
    "max": "#85FF31",
    "min": "#BC0000",
    "cardano": "#884EA0",
    "eos": "#BA68C8",
    "tether": "#FFC107",
    "xrp": "#1E88E5",
    "litecoin": "#3949AB",
    "bitcoin": "#0D47A1",
    "iota": "#FF33FF",
    "ethereum": "#E67E22",
    "stellar": "#5D6D7E"
}

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css',
                        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"]

# Initialise the app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.config.suppress_callback_exceptions = True

# ---------------------------------------- DASHBOARD STRUCTURE ------------------------------

# Define the app
app.layout = html.Div(
    children=[
        # Dashboard  Title
        html.H3(children=[
            html.I(className="fa fa-money"),
            html.B("Real time daily price predictions of cryptocurrency prices", style={
                   "paddingRight": "15px", "paddingLeft": "15px"}),
            html.I(className="fa fa-line-chart")
        ], style={"textAlign": "center"}),

        html.Br(),

        # First row
        html.Div(children=[
            # First row first crypto
            html.Div([
                html.Div([
                    # crypto selector
                    html.Div([
                        html.Div(children='Crypto selector: ', className="seven columns", style={
                            "textAlign": "right", "paddingTop": "5px"}),
                        html.Div(children=[
                            dcc.Dropdown(id='crypto_selector_1',
                                         options=[
                                             {'label': crypto.capitalize(), 'value': crypto} for crypto in cryptos],
                                         value="bitcoin",
                                         clearable=False,
                                         searchable=True
                                         )
                        ], className="five columns", style={"textAlign": "center"})
                    ], className="five columns"),

                    # white space
                    html.Div([], className="two columns"),

                    # days selector
                    html.Div([
                        html.Div(children='Days interval: ', className="eight columns", style={
                            "textAlign": "right", "paddingTop": "5px"}),
                        html.Div(children=[
                            dcc.Dropdown(id='days_selector_1',
                                         options=[
                                             {'label': str(day), 'value': day} for day in days],
                                         value=30,
                                         clearable=False
                                         )
                        ], className="four columns", style={"textAlign": "center"})
                    ], className="five columns")
                ], className="row"),
                # Graph 2
                html.Div(children=[dcc.Graph(id='crypto_plot_1', config={'displayModeBar': False})], style={
                         "textAlign": "center", "contentAlign": "center"})

            ], className="six columns"),

            # First row second crypto
            html.Div([
                html.Div([
                    # crypto selector
                    html.Div([
                        html.Div(children='Crypto selector: ', className="seven columns", style={
                            "textAlign": "right", "paddingTop": "5px"}),
                        html.Div(children=[
                            dcc.Dropdown(id='crypto_selector_2',
                                         options=[
                                             {'label': crypto.capitalize(), 'value': crypto} for crypto in cryptos],
                                         value="ethereum",
                                         clearable=False,
                                         searchable=True
                                         )
                        ], className="five columns", style={"textAlign": "center"})
                    ], className="five columns"),

                    # white space
                    html.Div([], className="two columns"),

                    # days selector
                    html.Div([
                        html.Div(children='Days interval: ', className="eight columns", style={
                            "textAlign": "right", "paddingTop": "5px"}),
                        html.Div(children=[
                            dcc.Dropdown(id='days_selector_2',
                                         options=[
                                             {'label': str(day), 'value': day} for day in days],
                                         value=30,
                                         clearable=False
                                         )
                        ], className="four columns", style={"textAlign": "center"})
                    ], className="five columns")
                ], className="row"),
                # Graph 1
                html.Div(children=[dcc.Graph(id='crypto_plot_2', config={'displayModeBar': False})], style={
                         "textAlign": "center", "contentAlign": "center"})
            ], className="six columns")
        ], className="row"),

        html.Br(),

        # Second row
        html.Div(children=[
            # Second row first crypto
            html.Div([
                html.Div([
                    # crypto selector
                    html.Div([
                        html.Div(children='Crypto selector: ', className="seven columns", style={
                            "textAlign": "right", "paddingTop": "5px"}),
                        html.Div(children=[
                            dcc.Dropdown(id='crypto_selector_3',
                                         options=[
                                             {'label': crypto.capitalize(), 'value': crypto} for crypto in cryptos],
                                         value="xrp",
                                         clearable=False,
                                         searchable=True
                                         )
                        ], className="five columns", style={"textAlign": "center"})
                    ], className="five columns"),

                    # white space
                    html.Div([], className="two columns"),

                    # days selector
                    html.Div([
                        html.Div(children='Days interval: ', className="eight columns", style={
                            "textAlign": "right", "paddingTop": "5px"}),
                        html.Div(children=[
                            dcc.Dropdown(id='days_selector_3',
                                         options=[
                                             {'label': str(day), 'value': day} for day in days],
                                         value=30,
                                         clearable=False
                                         )
                        ], className="four columns", style={"textAlign": "center"})
                    ], className="five columns")
                ], className="row"),
                # Graph 3
                html.Div(children=[dcc.Graph(id='crypto_plot_3', config={'displayModeBar': False})], style={
                         "textAlign": "center", "contentAlign": "center"})

            ], className="six columns"),

            # Second row second crypto
            html.Div([
                html.Div([
                    # crypto selector
                    html.Div([
                        html.Div(children='Crypto selector: ', className="seven columns", style={
                            "textAlign": "right", "paddingTop": "5px"}),
                        html.Div(children=[
                            dcc.Dropdown(id='crypto_selector_4',
                                         options=[
                                             {'label': crypto.capitalize(), 'value': crypto} for crypto in cryptos],
                                         value="tether",
                                         clearable=False,
                                         searchable=True
                                         )
                        ], className="five columns", style={"textAlign": "center"})
                    ], className="five columns"),

                    # white space
                    html.Div([], className="two columns"),

                    # days selector
                    html.Div([
                        html.Div(children='Days interval: ', className="eight columns", style={
                            "textAlign": "right", "paddingTop": "5px"}),
                        html.Div(children=[
                            dcc.Dropdown(id='days_selector_4',
                                         options=[
                                             {'label': str(day), 'value': day} for day in days],
                                         value=30,
                                         clearable=False
                                         )
                        ], className="four columns", style={"textAlign": "center"})
                    ], className="five columns")
                ], className="row"),
                # Graph 4
                html.Div(children=[dcc.Graph(id='crypto_plot_4', config={'displayModeBar': False})], style={
                         "textAlign": "center", "contentAlign": "center"})
            ], className="six columns")
        ], className="row"),

        # Trigger that refreshes the page
        html.Div(children=[dcc.Interval(
                 id='crypto_interval',
                 interval=2500,  # in milliseconds
                 n_intervals=0)]
                 )
    ], style={'maxWidth': '94vw', "marginRight": "auto", "marginLeft": "auto", "overflow": "hidden"})


# ---------------------------------------- CALLBACKS ----------------------------------------

# first graph
@app.callback(
    Output('crypto_plot_1', 'figure'),
    [Input('crypto_interval', 'n_intervals'), Input(
        'crypto_selector_1', 'value'), Input('days_selector_1', 'value')]
)
def refresh_plot(n_intervals, selected_crypto, selected_day):
    print("Refreshing plot " + str(n_intervals))

    query = 'SELECT * FROM ' + selected_crypto
    query_result = session.execute(query, timeout=None)
    data = pd.DataFrame(list(query_result))

    data = data.sort_values(by="date")
    data = data.tail(selected_day)

    today = datetime.today().strftime('%Y-%m-%d')

    today_price = data[data["date"] == today]["price"]

    data.loc[data["date"] == today, ["yhat_lower", "yhat_upper"]] = today_price

    days = data["date"]
    points_list = data["price"]

    y_upper = data["yhat_upper"]
    y_lower = data["yhat_lower"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=days, y=points_list,
                             mode='lines+markers',
                             name='Expected value',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors[selected_crypto]
                             )

                  )

    fig.add_trace(go.Scatter(x=days, y=y_lower,
                             mode='lines+markers',
                             name='Min',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors["max"]
                             )
                  ),

    fig.add_trace(go.Scatter(x=days, y=y_upper,
                             mode='lines+markers',
                             name='Max',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors["min"]
                             )
                  )

    current_price = points_list.tail(8).to_list()[0]

    fig.update_layout(
        title={
            'text': "Current <b>" + selected_crypto.capitalize() + "</b> price: " + str(current_price) + " USD",
            'y': 0.96,
            'x': 0.45,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Days",
        yaxis_title="Price (USD)",
        hovermode="x",
        margin_t=34,
    )

    return fig


# second graph
@app.callback(
    Output('crypto_plot_2', 'figure'),
    [Input('crypto_interval', 'n_intervals'), Input(
        'crypto_selector_2', 'value'), Input('days_selector_2', 'value')]
)

def refresh_plot(n_intervals, selected_crypto, selected_day):
    print("Refreshing plot " + str(n_intervals))

    query = 'SELECT * FROM ' + selected_crypto
    query_result = session.execute(query, timeout=None)
    data = pd.DataFrame(list(query_result))

    data = data.sort_values(by="date")
    data = data.tail(selected_day)

    today = datetime.today().strftime('%Y-%m-%d')

    today_price = data[data["date"] == today]["price"]

    data.loc[data["date"] == today, ["yhat_lower", "yhat_upper"]] = today_price

    days = data["date"]
    points_list = data["price"]

    y_upper = data["yhat_upper"]
    y_lower = data["yhat_lower"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=days, y=points_list,
                             mode='lines+markers',
                             name='Expected value',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors[selected_crypto]
                             )

                  )

    fig.add_trace(go.Scatter(x=days, y=y_lower,
                             mode='lines+markers',
                             name='Min',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors["max"]
                             )
                  ),

    fig.add_trace(go.Scatter(x=days, y=y_upper,
                             mode='lines+markers',
                             name='Max',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors["min"]
                             )
                  )

    current_price = points_list.tail(8).to_list()[0]

    fig.update_layout(
        title={
            'text': "Current <b>" + selected_crypto.capitalize() + "</b> price: " + str(current_price) + " USD",
            'y': 0.96,
            'x': 0.45,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Days",
        yaxis_title="Price (USD)",
        hovermode="x",
        margin_t=34
    )

    return fig


# third graph
@app.callback(
    Output('crypto_plot_3', 'figure'),
    [Input('crypto_interval', 'n_intervals'), Input(
        'crypto_selector_3', 'value'), Input('days_selector_3', 'value')]
)

def refresh_plot(n_intervals, selected_crypto, selected_day):
    print("Refreshing plot " + str(n_intervals))

    query = 'SELECT * FROM ' + selected_crypto
    query_result = session.execute(query, timeout=None)
    data = pd.DataFrame(list(query_result))

    data = data.sort_values(by="date")
    data = data.tail(selected_day)

    today = datetime.today().strftime('%Y-%m-%d')

    today_price = data[data["date"] == today]["price"]

    data.loc[data["date"] == today, ["yhat_lower", "yhat_upper"]] = today_price

    days = data["date"]
    points_list = data["price"]

    y_upper = data["yhat_upper"]
    y_lower = data["yhat_lower"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=days, y=points_list,
                             mode='lines+markers',
                             name='Expected value',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors[selected_crypto]
                             )

                  )

    fig.add_trace(go.Scatter(x=days, y=y_lower,
                             mode='lines+markers',
                             name='Min',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors["max"]
                             )
                  ),

    fig.add_trace(go.Scatter(x=days, y=y_upper,
                             mode='lines+markers',
                             name='Max',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors["min"]
                             )
                  )

    current_price = points_list.tail(8).to_list()[0]

    fig.update_layout(
        title={
            'text': "Current <b>" + selected_crypto.capitalize() + "</b> price: " + str(current_price) + " USD",
            'y': 0.96,
            'x': 0.45,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Days",
        yaxis_title="Price (USD)",
        hovermode="x",
        margin_t=34
    )

    return fig


# fourth graph
@app.callback(
    Output('crypto_plot_4', 'figure'),
    [Input('crypto_interval', 'n_intervals'), Input(
        'crypto_selector_4', 'value'), Input('days_selector_4', 'value')]
)
def refresh_plot(n_intervals, selected_crypto, selected_day):
    print("Refreshing plot " + str(n_intervals))

    query = 'SELECT * FROM ' + selected_crypto
    query_result = session.execute(query, timeout=None)
    data = pd.DataFrame(list(query_result))

    data = data.sort_values(by="date")
    data = data.tail(selected_day)

    today = datetime.today().strftime('%Y-%m-%d')

    today_price = data[data["date"] == today]["price"]

    data.loc[data["date"] == today, ["yhat_lower", "yhat_upper"]] = today_price

    days = data["date"]
    points_list = data["price"]

    y_upper = data["yhat_upper"]
    y_lower = data["yhat_lower"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=days, y=points_list,
                             mode='lines+markers',
                             name='Expected value',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors[selected_crypto]
                             )

                  )

    fig.add_trace(go.Scatter(x=days, y=y_lower,
                             mode='lines+markers',
                             name='Min',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors["max"]
                             )
                  ),

    fig.add_trace(go.Scatter(x=days, y=y_upper,
                             mode='lines+markers',
                             name='Max',
                             line_shape='spline',
                             line_width=1.5,
                             marker_size=4,
                             marker_color=crypto_colors["min"]
                             )
                  )

    current_price = points_list.tail(8).to_list()[0]

    fig.update_layout(
        title={
            'text': "Current <b>" + selected_crypto.capitalize() + "</b> price: " + str(current_price) + " USD",
            'y': 0.96,
            'x': 0.45,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="Days",
        yaxis_title="Price (USD)",
        hovermode="x",
        margin_t=34
    )

    return fig

# ----------------------------------------    MAIN   ----------------------------------------


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
