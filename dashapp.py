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

cluster = Cluster(["127.0.0.1"], port=9042)
session = cluster.connect('cryptos_keyspace',wait_for_all_pools=True)
session.execute("USE cryptos_keyspace")

cryptos = ["bitcoin", "ethereum", "tether", "xrp", "litecoin", "cardano", "iota", "eos", "stellar"]

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
        html.Center([html.H3("Real time daily price predictions of cryptocurrency prices")]),

        # Crypto selector
        html.Div(className="row", children=[

                    html.Div(children=[
                            html.Center(children=[html.P('Crypto selector')]),
                            html.Center(children=[html.Div(
                                className='row',
                                children =  [
                                                dcc.Dropdown(id='crypto_selector', options=[{'label': crypto, 'value': crypto} for crypto in cryptos],
                                                            multi=False, value="bitcoin",
                                                            className='crypto_selector',
                                                            clearable=False,
                                                            searchable=True,
                                                            style={"width": "60%",
                                                                    "verticalAlign": "middle"
                                                            }
                                                            )
                                            ])
                            ])
                        ])
                ]),

        # Main dashboard components
        html.Div(className='row',
                    children=  [
                                    html.Div(children=[
                                                dcc.Graph(id='crypto_plot', config={'displayModeBar': True}),
                                                dcc.Interval(
                                                    id='crypto_interval',
                                                    interval=1000, # in milliseconds
                                                    n_intervals=0
                                                )
                                            ])
                                ]
                ),

        ]
    )


# ---------------------------------------- CALLBACKS ----------------------------------------

@app.callback(Output('crypto_plot_Test', 'figure'),
              [Input('crypto_selector', 'value')])
def update_line_crypto(selected_dropdown_value):
    query = 'SELECT * FROM ' + selected_dropdown_value
    query_result = session.execute(query, timeout=None)
    data = pd.DataFrame(list(query_result))

    data = data.sort_values(by="date")
    data = data.tail(30)

    days = data["date"]
    points_list = data["price"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=days, y=points_list,
                        mode='lines+markers',
                        name='points',
                        line_shape='spline',
                        line_width=3,
                        marker_size=10,
                        )

                )

    fig.update_layout(
        title= str(selected_dropdown_value) + " price by day",
        title_x=0.5,
        xaxis_title="Day",
        yaxis_title="Price",
        hovermode="x"
        #transition=trx
    )

    return fig

@app.callback(
    Output('crypto_plot', 'figure'),
    [ Input('crypto_interval', 'n_intervals'), Input('crypto_selector', 'value')]
)
def refresh_plot(n_intervals, selected_dropdown_value):
    print("Refreshing plot " + str(n_intervals))

    query = 'SELECT * FROM ' + selected_dropdown_value
    query_result = session.execute(query, timeout=None)
    data = pd.DataFrame(list(query_result))

    data = data.sort_values(by="date")
    data = data.tail(30)

    days = data["date"]
    points_list = data["price"]

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=days, y=points_list,
                        mode='lines+markers',
                        name='points',
                        line_shape='spline',
                        line_width=3,
                        marker_size=10,
                        )

                )

    current_price = points_list.tail(8).to_list()[0]

    fig.update_layout(
        title= "Current " + selected_dropdown_value + " price: " + str(current_price) + " USD",
        title_x=0.5,
        xaxis_title="Day",
        yaxis_title="Price (USD)",
        hovermode="x"
        #transition=trx
    )

    return fig

# ----------------------------------------    MAIN   ----------------------------------------

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)