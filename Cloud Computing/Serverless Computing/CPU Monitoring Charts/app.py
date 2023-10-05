import redis
import json
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import plotly.graph_objs as go
import re
r = redis.Redis(host='192.168.121.189',port=6379)
external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css2?"
                "family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]
app = Dash(__name__, update_title=None, external_stylesheets=external_stylesheets)
app.title = "Server Memory and CPU Data"
def make_graphs():
    divs  = []
    divs.append([html.Div([
                html.P(children="🖥️", className="header-emoji"),
                html.H1(
                    children="Server Data", className="header-title"
                ),
                html.P(
                    children="Server CPUs Usage Moving Average and Memory Consumption",
                    className="header-description",
                ),
            ],
            className="header"
        )])
    divs.append([
        html.Div(style={'padding-left':'5%','padding-right':'5%'},children=[
            html.H1('Memory  Utilization ',style={'textAlign': 'center'}),
            dcc.Graph(id = '15-seconds-vm-graph', animate = False),
            dcc.Interval(
                id = '15-seconds-vm-update',
                interval = 100,
                n_intervals=0
            )
        ])])
    divs.append([html.H1('CPU Utilization Moving Averages', style={'textAlign': 'center'})])
    for i in range(14):
        divs.append(
            [
        html.Div(style={'padding-left':'10%'},children=[
            html.H1('CPU '+str(i+1)+' Utilization Moving Averages'),
            html.Div(className='container',style={'display': 'flex'},children=[
            dcc.Graph(id = '60-seconds-graph'+str(i), animate = False,style={'width': '45%'}),
            dcc.Interval(
                id = '60-seconds-update'+str(i),
                interval = 200,
                n_intervals=0
            )
        ,
            dcc.Graph(id = '60-minutes-graph'+str(i), animate = False,style={'width':'45%'}),
            dcc.Interval(
                id = '60-minutes-update'+str(i),
                interval = 10000,
                n_intervals=0
            )
        ])
    ])  ])
    
    divs = [x for xs in divs for x in xs]
    return divs

app.layout = html.Div(children=make_graphs())

def update_graph(cpu,n):
    data = r.get('julioferreira-proj3-output')
    data = json.loads(data)
    cpu = int(re.search(r'\d+', cpu[-3:]).group())
    data = go.Scatter(
            x=data["timestamp-60sec"],
            y=data["avg-usage-cpu-60sec"][cpu],
            
            name='Lines'+str(cpu),
            mode= 'lines',
            line = dict(color='green')
    )

    return {'data': [data],
            'layout': go.Layout(
            margin=go.layout.Margin(
            l=50,
            r=50,
            b=100,
            t=100,
            pad = 4),
            xaxis={'title': 'Timestamp', 'autorange': True,'linecolor':'black'},
            yaxis = dict(title = 'CPU Usage Moving Average (%)',linecolor='black',range=[0, 100]))}
    
def update_graph_60(cpu,n):
    data = r.get('julioferreira-proj3-output')
    data = json.loads(data)
    cpu = int(re.search(r'\d+', cpu[-3:]).group())
    data = go.Scatter(
            x=data["timestamp-60min"],
            y=data["avg-usage-cpu-60min"][cpu],
            name='Linesmin'+str(cpu),
            mode= 'lines+markers',
            line = dict(color='green'),
            marker = dict(color='green')
    )

    return {'data': [data],
            'layout': go.Layout(
            margin=go.layout.Margin(
            l=50,
            r=50,
            b=100,
            t=100,
            pad = 4),
            xaxis={'title': 'Timestamp', 'autorange': True,'linecolor':'black'},
            yaxis=dict(title = 'CPU Usage Moving Average (Hourly) (%)',linecolor='black',range=[0, 100]))} 
        
def update_graph_vm(n):
    data = r.get('julioferreira-proj3-output')
    data = json.loads(data)
    data = go.Scatter(
            x=data["timestamp-rt"],
            y=data["virtual-memory-percent-rt"],
            name='Lines',
            mode= 'lines',
            line = dict(color='green')
    )

    return {'data': [data],
            'layout': go.Layout(
            margin=go.layout.Margin(
            l=50,
            r=50,
            b=100,
            t=100,
            pad = 4),
            xaxis={'title': 'Timestamp', 'autorange': True,'linecolor':'black'},
            yaxis=dict(title = 'Memory Usage (%)',linecolor='black',range=[0, 100]))}
      
for cpu in range(14):
    app.callback(
        Output('60-seconds-graph'+str(cpu), 'figure'),
        [ Input('60-minutes-update'+str(cpu), 'id'),Input('60-minutes-update'+str(cpu), 'n_intervals')]
    )(update_graph)

    app.callback(
        Output('60-minutes-graph'+str(cpu), 'figure'),
        [ Input('60-minutes-update'+str(cpu), 'id'),Input('60-minutes-update'+str(cpu), 'n_intervals') ]
    )(update_graph_60)
    
app.callback(
        Output('15-seconds-vm-graph', 'figure'),
        [ Input('15-seconds-vm-update', 'n_intervals') ]
    )(update_graph_vm)
   
  
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=5118)