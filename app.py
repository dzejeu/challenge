import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from challenge.stats import declarants, trades
from datetime import datetime


declarants_list = declarants()

app = dash.Dash()

app.layout = html.Div([
    dcc.Dropdown(
        id='declarant',
        options=[{'label': dec, 'value': dec} for dec in declarants_list],
        value='FR'
    ),
    dcc.Dropdown(
        id='trade-type',
        options=[{'label': 'IMPORT', 'value': 'I'}, {'label': 'EXPORT', 'value': 'E'}],
        value='I'
    ),
    dcc.Graph(id='trades-euros'),
])


@app.callback(
    dash.dependencies.Output('trades-euros', 'figure'),
    [dash.dependencies.Input('declarant', 'value'),
     dash.dependencies.Input('trade-type', 'value')]
)
def update_figure(declarant, trade_type):
    df = trades(declarant, trade_type, datetime(2015, 1, 1), datetime(2018, 5, 1))
    return {
        'data': [
            go.Scatter(
                x=df['Month'],
                y=df['Trades'],
                mode='lines+markers',
                marker={
                    'size': 12,
                    'opacity': 0.5,
                    'line': {'width': 2, 'color': 'blue'}
                },
                name='Euros',
                yaxis='y'
            ),
            go.Scatter(
                x=df['Month'],
                y=df['MovingAvg'],
                mode='lines+markers',
                marker={
                    'size': 12,
                    'opacity': 0.5,
                    'line': {'width': 2, 'color': 'blue'}
                },
                name='Moving Average',
                yaxis='y'
            ),
            go.Bar(
                x=df['Month'],
                y=df['YoY'],
                name='YoY',
                yaxis='y2'
            ),
            go.Bar(
                x=df['Month'],
                y=df['MoM'],
                name='MoM',
                yaxis='y2'
            )
        ],
        'layout': go.Layout(
            xaxis={'title': 'Month'},
            yaxis={'title': 'Trades in euros', 'range': [df['Trades'].min() * 0.8, df['Trades'].max() * 1.2]},
            yaxis2={'title': '% Changes',
                    'range': [min(df['YoY'].min(), df['MoM'].min()) * 0.9,
                              max(df['MoM'].max(), df['YoY'].max()) * 3],
                    'side': 'right', 'overlaying': 'y'},
            margin={'l': 70, 'b': 40, 't': 50, 'r': 60},
            legend={'x': 0, 'y': 1},
            hovermode='closest'
        )
    }


if __name__ == '__main__':
    app.run_server(port=80, host='0.0.0.0')
