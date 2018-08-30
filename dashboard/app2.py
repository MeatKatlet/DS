import dash
import dash_html_components as html
import dash_core_components as dcc
from datetime import datetime as dt
import numpy as np

from dash.dependencies import Input, Output

app = dash.Dash()
app.config['suppress_callback_exceptions']=True
N = 100
random_x = np.linspace(0, 1, N)
random_y0 = np.random.randn(N)+5
random_y1 = np.random.randn(N)

app.layout = html.Div([
    dcc.Tabs(id="tabs", value='tab-1', children=[
        dcc.Tab(label='Tab one', value='tab-1'),
        dcc.Tab(label='Tab two', value='tab-2'),
    ]),
    html.Div(id='tabs-content')
])

@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-1':
        return html.Div([
            html.H3('Tab content 1')

        ])
    elif tab == 'tab-2':
        return html.Div([
            html.H3('Tab content 2'),
            html.Div([
                html.H1(children='Имя графика'),
                html.Div([dcc.DatePickerRange(
                    id='date-picker-range',
                    start_date=dt(2018, 7, 22),
                    end_date_placeholder_text='Select a date!'
                )]),
                html.Div([
                    dcc.Graph(
                        id='example-graph',
                        figure={
                            'data': [{
                                "x": random_x,
                                "y": random_y0,
                                "mode": 'lines',
                                "name": 'lines'
                            },
                                {
                                    "x": random_x,
                                    "y": random_y1,
                                    "mode": 'lines',
                                    "name": 'lines'
                                }
                            ],
                            'layout': {
                                'title': 'Data Visualization'
                            }
                        }
                    )
                    ]),
            ]),

        ])


@app.callback(
    Output('example-graph', 'figure'),
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date')]
)
def update_graph(start_date,end_date):
    N = 100
    random_x = np.linspace(0, 1, N)
    random_y0 = np.random.randn(N) + 5
    random_y1 = np.random.randn(N)
    return {
        'data': [{
                "x" : random_x,
                "y" : random_y0,
                "mode" : 'lines',
                "name" : 'lines'
            },
            {
                "x": random_x,
                "y": random_y1,
                "mode": 'lines',
                "name": 'lines'
            }
        ],
        'layout': {
            'title': 'Data Visualization'
        }

    }

if __name__ == '__main__':
    app.run_server(debug=True)