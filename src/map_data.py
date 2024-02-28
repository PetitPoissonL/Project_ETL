import pandas as pd
import plotly.express as px
from config import load_config

'''
Data visualization program that presents the number of occurrences on a timeline on a map. 
The map will be opened in a web browser.
'''


def get_dataset(path):
    df = pd.read_csv(path)
    df['eventDate'] = pd.to_datetime(df['eventDate'])
    df = df[['eventDate', 'decimalLatitude', 'decimalLongitude', 'individualCount']]
    df.sort_values('eventDate', inplace=True)
    return df


def draw_mapbox(dataframe):
    figure = px.density_mapbox(
        dataframe,
        lat='decimalLatitude',
        lon='decimalLongitude',
        z='individualCount',
        radius=10,
        center=dict(lat=0, lon=180),
        zoom=0,
        mapbox_style="open-street-map",
        animation_frame='eventDate',
        labels={'eventDate': 'Event date'}
    )
    return figure


def update_layout(figure):
    figure.update_layout(
        title_text='Occurrences of Panthera leo in the world',
        title_x=0.5,
        title_font_size=24,
        # Adjusts playback speed
        updatemenus=[dict(
            type="buttons",
            showactive=False,
            buttons=[
                dict(method="animate",
                     args=[None, {"frame": {"duration": 100, "redraw": True}, "fromcurrent": True}]),
                dict(method="animate",
                     args=[[None], {"frame": {"duration": 0, "redraw": True}, "mode": "immediate"}])
            ]
        )]
    )

    figure.update_coloraxes(
        colorbar_title='Individual count'
    )


def show_mapbox(figure):
    # Displays the map in the browser
    figure.show(renderer="browser")


def main():
    config = load_config()
    data_path = config['configuration']['path_file'] + config['configuration']['dataset_visualization']
    dataframe = get_dataset(data_path)
    fig = draw_mapbox(dataframe)
    update_layout(fig)
    show_mapbox(fig)


if __name__ == "__main__":
    main()
