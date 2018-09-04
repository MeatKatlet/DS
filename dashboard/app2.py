import dash
import dash_html_components as html
import dash_core_components as dcc
from datetime import datetime as dt
import numpy as np
from dashboard.elastic_logic.Elastic_time_series_ratio_calculator import Elastic_time_series_ratio_calculator
from dashboard.app_logic.Data_manipulator import Data_manipulator
import json
import time
import datetime
import plotly.graph_objs as go

from dash.dependencies import Input, Output

app = dash.Dash()
app.config['suppress_callback_exceptions']=True
N = 100
random_x = np.linspace(0, 1, N)
random_y0 = np.random.randn(N)+5
random_y1 = np.random.randn(N)

#todo создать
with open('../searches/brand_dict_matrix.json') as data_file:
    brand_dict = json.load(data_file)
with open('../searches/group_dict_matrix.json') as data_file:
    group_dict = json.load(data_file)
with open('../searches/region_dict_matrix.json') as data_file:
    region_dict = json.load(data_file)


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

            html.Div([

                html.Div([
                    dcc.DatePickerRange(
                    id='date-picker-range',
                    start_date=dt(2018, 7, 18),
                    end_date=dt(2018, 7, 19),
                    end_date_placeholder_text='Select a date!'
                ),
                dcc.Dropdown(
                    id='time_bucket',
                    options=[
                            {'label': '5 Минут', 'value': '5m'},
                            {'label': '30 Минут', 'value': '30m'},
                            {'label': '1 Час', 'value': '1h'},
                            {'label': '3 Часа', 'value': '3h'},
                            {'label': '6 Часов', 'value': '6h'},
                            {'label': '12 Часов', 'value': '12h'},
                            {'label': '1 День', 'value': '1d'},
                            {'label': '1 Неделя', 'value': '1w'},
                            {'label': '1 Месяц', 'value': '1M'}
                    ],
                    value='30m'
                ),
                html.Div([
                    html.H4('Следующие 3 элемента управления производят предварительную фильтрацию данных из бд. Графики будут строиться только по выбранным городам, брендам, товарным группам. Если ничего в них не выбрано, то выбираются соответственно все города/бренды/группы.'),
                    html.H5('Выбрать данные по региону(ам):'),
                    dcc.Dropdown(
                            id='my-dropdown-region',
                            options=[{'label': key, 'value': region_dict[key]} for key in region_dict],
                            multi=True,
                            value=region_dict["Москва"]
                    ),
                    html.H5('Выбрать данные по бренду(ам):'),
                    dcc.Dropdown(
                            id='my-dropdown-brand',
                            options=[{'label': key, 'value': brand_dict[key]} for key in brand_dict],
                            multi=True,
                            value=[]
                    ),
                    html.H5('Выбрать данные по товарной группе(ам):'),
                    dcc.Dropdown(
                            id='my-dropdown-group',
                            options=[{'label': key, 'value': group_dict[key]} for key in group_dict],
                            multi=True,
                            value=[]
                    ),
                ]),
                html.Div([
                    html.H5('Выбрать способ группировки:'),
                    dcc.Dropdown(
                        id='group_field',
                        options=[
                            {'label': 'По товарной группе', 'value': 'gr'},
                            {'label': 'По бренду', 'value': 'br'}
                        ],
                        multi=True,
                        value='gr'
                    ),
                ])

                ]),

                html.Div([
                    dcc.Graph(
                        id='example-graph'

                    ),
                    dcc.Graph(
                        id='example-graph2',
                        figure={
                            'data': [],
                            'layout': {
                                'title': 'Удачных поисков, Всего поисков'
                            }
                        }
                    ),
                    dcc.Graph(
                        id='example-graph3',
                        figure={
                            'data': [],
                            'layout': {
                                'title': 'Количество раз за период когда поиск отвечал отсутствием. Разница м/у всего поисками и успешными.'
                            }
                        }
                    )
                    ]),
            ]),

        ])


@app.callback(
    Output('example-graph', 'figure'),
    [
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
        Input('time_bucket', 'value'),
        Input('my-dropdown-region', 'value'),
        Input('my-dropdown-brand', 'value'),
        Input('my-dropdown-group', 'value'),
        Input('group_field', 'value'),

     ]
)
def update_graph(start_date,end_date,time_bucket, regions,brands,groups,group_fields):
    #N = 100
    global region_dict,brand_dict,group_dict
    #time.mktime(datetime.datetime.strptime(s, "%d/%m/%Y").timetuple())

    start_timestamp = int(time.mktime(datetime.datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    end_timestamp = int(time.mktime(datetime.datetime.strptime(end_date, "%Y-%m-%d").timetuple()))

    #todo для ускорения
    if isinstance(regions, str):
        regions = [regions]
    if isinstance(brands, str):
        brands = [brands]
    if isinstance(groups, str):
        groups = [groups]


    if isinstance(group_fields, str):
        group_fields = [group_fields]
    #random_x = np.linspace(0, 1, N)

    #random_y0 = np.random.randn(N) + 5
    #random_y1 = np.random.randn(N)

    query1 = Elastic_time_series_ratio_calculator(
        from_timestamp=start_timestamp,
        to_timestamp=end_timestamp,
        filtered_regions=regions,
        filtered_brand=brands,
        filtered_groups=groups,
        field_for_grouping=group_fields,
        metric_field="suc",
        interval_to_aggregate=time_bucket
    )
    q = query1.query_for_searches

    query1.get_aggregated_data(q,size=0)

    query2 = Elastic_time_series_ratio_calculator(
        from_timestamp=start_timestamp,
        to_timestamp=end_timestamp,
        filtered_regions=regions,
        filtered_brand=brands,
        filtered_groups=groups,
        field_for_grouping=group_fields,
        metric_field="t",
        interval_to_aggregate=time_bucket
    )
    q = query2.query_for_searches

    query2.get_aggregated_data(q,size=0)

    #посчитать отношение м/у двумя рядами! и этот результат будет значения по y для одной группы

    manipulator = Data_manipulator(brand_dict,group_dict,group_fields)
    ratio_series = manipulator.calc_absolute_filtered(query1.result, query2.result)
    layout = go.Layout(
        title='% Неудачных поисков (неудачные поиски/всего поисков)'
        #margin=go.Margin(l=50, r=50, b=50, t=50),
        #yaxis={'title': yaxis_title}
    )
    return {
        'data': ratio_series,
        'layout': layout
    }



@app.callback(
    Output('example-graph2', 'figure'),
    [
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
        Input('time_bucket', 'value'),
        Input('my-dropdown-region', 'value'),
        Input('my-dropdown-brand', 'value'),
        Input('my-dropdown-group', 'value'),
        Input('group_field', 'value'),
     ]
)
def update_graph2(start_date,end_date,time_bucket, regions,brands,groups,group_fields):
    global region_dict, brand_dict, group_dict

    start_timestamp = int(time.mktime(datetime.datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    end_timestamp = int(time.mktime(datetime.datetime.strptime(end_date, "%Y-%m-%d").timetuple()))


    if isinstance(regions, str):
        regions = [regions]
    if isinstance(brands, str):
        brands = [brands]
    if isinstance(groups, str):
        groups = [groups]

    if isinstance(group_fields, str):
        group_fields = [group_fields]

    query1 = Elastic_time_series_ratio_calculator(
        from_timestamp=start_timestamp,
        to_timestamp=end_timestamp,
        filtered_regions=regions,
        filtered_brand=brands,
        filtered_groups=groups,
        field_for_grouping=group_fields,
        metric_field="suc",
        interval_to_aggregate=time_bucket
    )
    q = query1.query_for_searches

    query1.get_aggregated_data(q, size=0)

    query2 = Elastic_time_series_ratio_calculator(
        from_timestamp=start_timestamp,
        to_timestamp=end_timestamp,
        filtered_regions=regions,
        filtered_brand=brands,
        filtered_groups=groups,
        field_for_grouping=group_fields,
        metric_field="t",
        interval_to_aggregate=time_bucket
    )
    q = query2.query_for_searches

    query2.get_aggregated_data(q, size=0)

    manipulator = Data_manipulator(brand_dict,group_dict)
    ratio_series = manipulator.calc_absolute(query1.result, query2.result)

    return {
        'data': ratio_series,
        'layout': {
            'title': 'Удачных поисков, Всего поисков'
        }

    }

@app.callback(
    Output('example-graph3', 'figure'),
    [
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
        Input('time_bucket', 'value'),
        Input('my-dropdown-region', 'value'),
        Input('my-dropdown-brand', 'value'),
        Input('my-dropdown-group', 'value'),
        Input('group_field', 'value'),
     ]
)
def update_graph3(start_date,end_date,time_bucket, regions,brands,groups,group_fields):
    global region_dict, brand_dict, group_dict

    start_timestamp = int(time.mktime(datetime.datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    end_timestamp = int(time.mktime(datetime.datetime.strptime(end_date, "%Y-%m-%d").timetuple()))


    if isinstance(regions, str):
        regions = [regions]
    if isinstance(brands, str):
        brands = [brands]
    if isinstance(groups, str):
        groups = [groups]

    if isinstance(group_fields, str):
        group_fields = [group_fields]

    query1 = Elastic_time_series_ratio_calculator(
        from_timestamp=start_timestamp,
        to_timestamp=end_timestamp,
        filtered_regions=regions,
        filtered_brand=brands,
        filtered_groups=groups,
        field_for_grouping=group_fields,
        metric_field="suc",
        interval_to_aggregate=time_bucket
    )
    q = query1.query_for_searches

    query1.get_aggregated_data(q, size=0)

    query2 = Elastic_time_series_ratio_calculator(
        from_timestamp=start_timestamp,
        to_timestamp=end_timestamp,
        filtered_regions=regions,
        filtered_brand=brands,
        filtered_groups=groups,
        field_for_grouping=group_fields,
        metric_field="t",
        interval_to_aggregate=time_bucket
    )
    q = query2.query_for_searches

    query2.get_aggregated_data(q, size=0)

    manipulator = Data_manipulator(brand_dict,group_dict)
    ratio_series = manipulator.calc_diff(query1.result, query2.result)

    return {
        'data': ratio_series,
        'layout': {
            'title': 'Количество раз за период когда поиск отвечал отсутствием. Разница м/у всего поисками и успешными.'
        }

    }



if __name__ == '__main__':
    app.run_server(debug=True)